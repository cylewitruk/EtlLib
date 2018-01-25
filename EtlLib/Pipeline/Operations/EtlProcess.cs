using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Nodes.Impl;
using EtlLib.Support;

namespace EtlLib.Pipeline.Operations
{
    internal class EtlProcess<TOut> : EtlProcess, IEtlOperationWithEnumerableResult<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        protected internal EtlProcess(IInputOutputAdapter[] ioAdapters) : base(ioAdapters)
        {
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            var baseResult = base.Execute(context);

            var collector = (GenericResultCollectionNode<TOut>) ResultCollector;
            var result = new EnumerableEtlOperationResult<TOut>(baseResult.IsSuccess, collector.Result);
            return result;
        }
    }
    
    internal class EtlProcess : AbstractEtlOperationWithNoResult, IDisposable
    {
        private readonly ILogger _log;
        private readonly List<IInputOutputAdapter> _ioAdapters;
        private readonly List<INode> _nodes;
        private readonly NodeStatistics _nodeStatistics;
        private readonly IErrorHandler _errorHandler;
        private readonly ConcurrentBag<EtlOperationError> _errors;

        protected INodeWithInput ResultCollector { get; }
        protected bool HasResult { get; }

        protected internal EtlProcess(IInputOutputAdapter[] ioAdapters)
        {
            _log = EtlLibConfig.LoggingAdapter.CreateLogger("EtlLib.EtlProcess");
            _ioAdapters = new List<IInputOutputAdapter>(ioAdapters);
            _nodes = new List<INode>(ioAdapters.Select(x => x.OutputNode).Concat(ioAdapters.SelectMany(x => x.AttachedNodes)).Distinct());
            _nodeStatistics = new NodeStatistics();
            _errors = new ConcurrentBag<EtlOperationError>();
            _errorHandler = new ErrorHandler()
            {
                OnItemErrorFn = (n, e, i) =>
                {
                    _log.Error(e.Message);
                    _nodeStatistics.IncrementErrors(n);
                    _errors.Add(new EtlOperationError(this, n, e, i));

                    if (EtlLibConfig.EnableDebug)
                        Debugger.Break();
                },
                OnErrorFn = (n, e) =>
                {
                    _log.Error(e.Message);
                    _nodeStatistics.IncrementErrors(n);
                    _errors.Add(new EtlOperationError(this, n, e));

                    if (EtlLibConfig.EnableDebug)
                        Debugger.Break();
                }
            };

            foreach (var node in _nodes)
            {
                node.SetErrorHandler(_errorHandler);
                _nodeStatistics.RegisterNode(node);

                if (node is IResultCollectorNode collector)
                {
                    HasResult = true;
                    ResultCollector = collector;
                }
            }

            foreach (var adapter in _ioAdapters)
            {
                adapter.SetNodeStatisticsCollector(_nodeStatistics);
            }
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            _log.Info(new string('=', 80));
            _log.Info($"= Executing ETL Process '{Name}' (Started {DateTime.Now})");
            _log.Info(new string('=', 80));

            var elapsedDict = new ConcurrentDictionary<INode, TimeSpan>();

            var tasks = new List<Task>();
            var processStopwatch = Stopwatch.StartNew();

            foreach (var node in _nodes)
            {
                var task = Task.Factory.StartNew(() =>
                //var task = Task.Run(() =>
                {
                        _log.Info($"Beginning execute task for node {node}.");
                        var sw = Stopwatch.StartNew();

                        try
                        {
                            if (node is INodeWithOutput outputNode && context.ObjectPool.HasObjectPool(outputNode.OutputType))
                            {
                                var ioAdapter = _ioAdapters.SingleOrDefault(x => x.OutputNode == node);
                                ioAdapter.SetObjectPool(context.ObjectPool.GetObjectPool(outputNode.OutputType));
                            }

                            node.Execute(context);
                        }
                        catch (Exception e)
                        {
                            _errorHandler.RaiseError(node, e);
                        }

                        sw.Stop();

                        _log.Info($"Execute task for node {node} has completed in {sw.Elapsed}.");
                        elapsedDict[node] = sw.Elapsed;

                    });

                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());

            processStopwatch.Stop();

            _log.Info(new string('=', 80));
            _log.Info($"= ETL Process '{Name}' has completed (Runtime {processStopwatch.Elapsed})");
            _log.Info(new string('=', 80));

            _log.Info("= EXECUTION STATISTICS");
            _log.Info("= Nodes:");
            var elapsedStats = elapsedDict.OrderBy(x => x.Value.TotalMilliseconds).ToArray();
            for (var i = 0; i < elapsedStats.Length; i++)
            {
                var sb = new StringBuilder();
                sb.Append($"= * {elapsedStats[i].Key} => Elapsed: {elapsedStats[i].Value}");

                var nodeStats = _nodeStatistics.GetNodeStatistics(elapsedStats[i].Key);
                sb.Append($" [R={nodeStats.Reads}, W={nodeStats.Writes}, E={nodeStats.Errors}]");

                if (i == 0)
                    sb.Append(" (fastest)");
                else if (i == elapsedStats.Length - 1)
                    sb.Append(" (slowest)");
                
                _log.Info(sb.ToString());
            }
            _log.Info($"= Total Reads:  {_nodeStatistics.TotalReads}");
            _log.Info($"= Total Writes: {_nodeStatistics.TotalWrites}");
            _log.Info($"= Total Errors: {_nodeStatistics.TotalErrors}");
            _log.Info(new string('=', 80));

            return new EtlOperationResult(_errors.Count == 0)
                .WithErrors(_errors);
        }

        public void Dispose()
        {
            _log.Debug("Disposing of all input/output adapters.");
            _ioAdapters.ForEach(x => x.Dispose());
            _nodes.Clear();
        }
    }
}