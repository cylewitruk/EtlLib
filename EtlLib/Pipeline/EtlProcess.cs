using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class EtlProcess
    {
        private readonly EtlProcessContext _processContext;
        private readonly ILoggingAdapter _loggingAdapter;
        private readonly ILogger _log;
        private readonly List<IInputOutputAdapter> _ioAdapters;
        private readonly List<string> _attachmentDeduplicationList;
        private readonly List<INode> _nodes;

        public INodeWithOutput RootNode { get; }
        public string Name { get; }

        public EtlProcess(EtlProcessContext context, ILoggingAdapter loggingAdapter)
        {
            _ioAdapters = new List<IInputOutputAdapter>();
            _attachmentDeduplicationList = new List<string>();
            _nodes = new List<INode>();

            _loggingAdapter = loggingAdapter;
            _processContext = context;

            _log = loggingAdapter.CreateLogger("EtlLib.EtlProcess");
        }

        private void RegisterNode(INode node)
        {
            if (_nodes.Contains(node))
                return;

            node.SetContext(_processContext);
            _nodes.Add(node);
        }

        private void RegisterNodes(params INode[] nodes)
        {
            foreach(var node in nodes)
                RegisterNode(node);
        }

        public void AttachInputToOutput<T>(INodeWithOutput<T> output, INodeWithInput<T> input) where T : class, IFreezable
        {
            var dedupHash = $"{output.Id}:{input.Id}";

            if (_attachmentDeduplicationList.Contains(dedupHash))
            {
                _log.Debug($"Node {input} is already attached to output {output}");
                return;
            }

            RegisterNodes(input, output);

            var ioAdapter = _ioAdapters.SingleOrDefault(x => x.OutputNode.Equals(output)) as InputOutputAdapter<T>;
            if (ioAdapter == null)
            {
                ioAdapter = new InputOutputAdapter<T>(output)
                    .WithLogger(_loggingAdapter.CreateLogger("EtlLib.IOAdapter"));

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
            
            _attachmentDeduplicationList.Add($"{output.Id}:{input.Id}");
        }

        public async Task Execute()
        {
            _log.Info($"=== Executing ETL Process '{Name}' ===");
            var tasks = new List<Task>();

            foreach (var node in _nodes)
            {
                _log.Info($"Beginning execute task for node {node}.");
                var task = node.Execute()
                    .ContinueWith(tsk => _log.Info($"Execute task for node {node} has completed."));
                task.Start();

                //var task = Task.Factory
                //    .StartNew(() => _log.Info($"Beginning execute task for node {node}."))
                //    .ContinueWith(tsk => node.Execute())
                //    .ContinueWith(tsk => _log.Info($"Execute task for node {node} has completed."));

                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            _log.Info($"=== ETL Process '{Name}' has completed ===");
        }
    }
}