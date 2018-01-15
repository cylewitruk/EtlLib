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
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline
{
    public class EtlProcess : IExecutableNode, IDisposable
    {
        private readonly EtlProcessContext _processContext;
        private readonly ILogger _log;
        private readonly List<IInputOutputAdapter> _ioAdapters;
        private readonly List<string> _attachmentDeduplicationList;
        private readonly List<INode> _nodes;
        private readonly EtlProcessSettings _settings;
        private readonly NodeStatistics _nodeStatistics;
        private readonly IErrorHandler _errorHandler;

        public INodeWithOutput RootNode { get; }
        public string Name { get; }

        public EtlProcess(EtlProcessSettings settings, EtlProcessContext context)
        {
            _settings = settings;

            Name = settings.Name;

            _ioAdapters = new List<IInputOutputAdapter>();
            _attachmentDeduplicationList = new List<string>();
            _nodes = new List<INode>();
            _nodeStatistics = new NodeStatistics();
            _errorHandler = new ErrorHandler()
            {
                OnItemErrorFn = (n, e, i) =>
                {
                    _log.Error(e.Message);
                    _nodeStatistics.IncrementErrors(n);
                },
                OnErrorFn = (n, e) =>
                {
                    _log.Error(e.Message);
                    _nodeStatistics.IncrementErrors(n);
                }
            };
            
            _processContext = context;

            _log = settings.LoggingAdapter.CreateLogger("EtlLib.EtlProcess");
        }

        private void RegisterNode(INode node)
        {
            if (_nodes.Contains(node))
                return;

            node
                .SetContext(_processContext)
                .SetErrorHandler(_errorHandler);
            _nodes.Add(node);
            _nodeStatistics.RegisterNode(node);
        }

        private void RegisterNodes(params INode[] nodes)
        {
            foreach(var node in nodes)
                RegisterNode(node);
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
                ioAdapter = new InputOutputAdapter<T>(_processContext, output, _nodeStatistics)
                    .WithLogger(_settings.LoggingAdapter.CreateLogger("EtlLib.IOAdapter"));

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

            input.SetWaiter(ioAdapter.Waiter);
            
            _attachmentDeduplicationList.Add($"{output.Id}:{input.Id}");
        }

        public void Execute()
        {
            _log.Info(new string('=', 80));
            _log.Info($"= Executing ETL Process '{Name}' (Started {DateTime.Now})");
            _log.Info(new string('=', 80));

            if (_settings.ContextInitializer != null)
            {
                _log.Info("Running context initializer.");
                _settings.ContextInitializer(_processContext);
            }

            var elapsedDict = new ConcurrentDictionary<INode, TimeSpan>();

            var tasks = new List<Task>();
            var processStopwatch = Stopwatch.StartNew();


            foreach (var node in _nodes)
            {
                var task = Task.Run(() =>
                    {
                        _log.Info($"Beginning execute task for node {node}.");
                        var sw = Stopwatch.StartNew();
                        node.Execute();
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
        }

        public void Dispose()
        {
            _log.Debug("Disposing of all input/output adapters.");
            _ioAdapters.ForEach(x => x.Dispose());
        }
    }
}