using System;
using System.Collections;
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
    public class EtlProcess<TOut> : EtlProcess, IEtlOperationWithEnumerableResult<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        public override IEtlOperationResult Execute()
        {
            var baseResult = base.Execute();

            var collector = (GenericResultCollectionNode<TOut>) ResultCollector;
            var result = new EnumerableEtlOperationResult<TOut>(baseResult.IsSuccess, collector.Result);
            return result;
        }
    }

    //TODO: Maybe break this out to EtlProcessDescriptor and have separate executors for noresult vs. enumerableresult?
    public class EtlProcess : AbstractEtlOperationWithNoResult, IDisposable
    {
        private readonly ILogger _log;
        private readonly List<IInputOutputAdapter> _ioAdapters;
        private readonly List<string> _attachmentDeduplicationList;
        private readonly List<INode> _nodes;
        private readonly NodeStatistics _nodeStatistics;
        private readonly IErrorHandler _errorHandler;
        private readonly ConcurrentBag<EtlOperationError> _errors;

        protected INodeWithInput ResultCollector { get; private set; }
        protected bool HasResult { get; private set; }

        public EtlProcess()
        {
            _log = EtlLibConfig.LoggingAdapter.CreateLogger("EtlLib.EtlProcess");
            _ioAdapters = new List<IInputOutputAdapter>();
            _attachmentDeduplicationList = new List<string>();
            _nodes = new List<INode>();
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
        }

        private void RegisterNode(INode node)
        {
            if (_nodes.Contains(node))
                return;

            node
                .SetErrorHandler(_errorHandler);
            _nodes.Add(node);
            _nodeStatistics.RegisterNode(node);
        }

        private void RegisterNodes(params INode[] nodes)
        {
            foreach(var node in nodes)
                RegisterNode(node);
        }

        private T BorrowObject<T>()
            where T : class, IResettable, new()
        {
            return Context.ObjectPool.Borrow<T>();
        }

        public void AttachInputToOutput<T>(INodeWithOutput<T> output, INodeWithInput<T> input) 
            where T : class, INodeOutput<T>, new()
        {
            var dedupHash = $"{output.Id}:{input.Id}";

            if (_attachmentDeduplicationList.Contains(dedupHash))
            {
                _log.Debug($"Node {input} is already attached to output {output}");
                return;
            }

            RegisterNodes(input, output);

            if (!(_ioAdapters.SingleOrDefault(x => x.OutputNode.Equals(output)) is InputOutputAdapter<T> ioAdapter))
            {
                ioAdapter = new InputOutputAdapter<T>(output, _nodeStatistics, BorrowObject<T>);

                output.SetEmitter(ioAdapter);

                _ioAdapters.Add(ioAdapter);
            }

            if (input is INodeWithInput2<T> input2)
            {
                ioAdapter.AttachConsumer(input2);

                if (input2.Input == null)
                {
                    _log.Info($"Attaching {input} input port #1 to output port of {output}.");
                    input.SetInput(ioAdapter.GetConsumingEnumerable(input));
                }
                else if (input2.Input2 == null)
                {
                    _log.Info($"Attaching {input} input port #2 to output port of {output}.");
                    input2.SetInput2(ioAdapter.GetConsumingEnumerable(input2));
                }
            }
            else
            {
                ioAdapter.AttachConsumer(input);

                _log.Info($"Attaching {input} input port #1 to output port of {output}.");
                input.SetInput(ioAdapter.GetConsumingEnumerable(input));
            }

            if (input is GenericResultCollectionNode<T> collector)
            {
                HasResult = true;
                ResultCollector = collector;
            }

            input.SetWaiter(ioAdapter.Waiter);
            
            _attachmentDeduplicationList.Add($"{output.Id}:{input.Id}");
        }

        public override IEtlOperationResult Execute()
        {
            _log.Info(new string('=', 80));
            _log.Info($"= Executing ETL Process '{Name}' (Started {DateTime.Now})");
            _log.Info(new string('=', 80));

            var elapsedDict = new ConcurrentDictionary<INode, TimeSpan>();

            var tasks = new List<Task>();
            var processStopwatch = Stopwatch.StartNew();


            foreach (var node in _nodes)
            {
                var task = Task.Run(() =>
                    {
                        _log.Info($"Beginning execute task for node {node}.");
                        var sw = Stopwatch.StartNew();

                        try
                        {
                            node.Execute(Context);
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
                var ioAdapter = _ioAdapters.SingleOrDefault(x => x.OutputNode == elapsedStats[i].Key);

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