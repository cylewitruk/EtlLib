using System;
using System.Collections.Generic;
using System.Linq;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessBuilder
    {
        Guid Id { get; }
        string Name { get; }

        IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlProcessContext, INodeWithOutput<TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();
    }

    /// <summary>
    /// An EtlProcess is designed to encapsulate a process which can execute independently of any other process and can freely stream results through the process's
    /// nodes.
    /// </summary>
    public class EtlProcessBuilder : IEtlProcessBuilder
    {
        //private readonly EtlPipelineContext _pipelineContext;
        private readonly EtlProcessSettings _settings;

        public Guid Id { get; }
        private readonly ILoggingAdapter _loggingAdapter;
        internal ILogger Log { get; }
        internal EtlProcessContext ProcessContext { get; }
        public string Name { get; private set; }

        internal Dictionary<Guid, InputOutputMap> NodeGraph { get; }
        internal Dictionary<Guid, EtlProcessBuilder> SubProcesses { get; }
        internal INode FirstNode { get; private set; }
        internal INode LastNode { get; private set; }

        private EtlProcessBuilder(EtlProcessSettings config)
        {
            _settings = config;
            Id = Guid.NewGuid();
            Name = config.Name ?? Id.ToString();
            ProcessContext = new EtlProcessContext(config.LoggingAdapter);
            NodeGraph = new Dictionary<Guid, InputOutputMap>();
            SubProcesses = new Dictionary<Guid, EtlProcessBuilder>();
            Log = config.LoggingAdapter.CreateLogger("EtlLib.EtlProcessBuilder");
            _loggingAdapter = config.LoggingAdapter;
        }

        public static IEtlProcessBuilder Create(Action<EtlProcessSettings> cfg)
        {
            var config = new EtlProcessSettings
            {
                LoggingAdapter = new NullLoggerAdapter()
            };

            cfg(config);

            var builder = new EtlProcessBuilder(config);
            builder.Log.Debug($"Created new EtlProcessBuilder '{builder.Name}'");
            return builder;
        }

        public IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlProcessContext, INodeWithOutput<TOut>> ctx) 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(ProcessContext);
            FirstNode = node;

            RegisterNode(node, (m, last) =>
            {
                Log.Debug($"'{Name}' registered new input {node}");
            });

            return new OutputNodeBuilderContext<TOut>(this, node);
        }

        /// <summary>
        /// Delegate for node registration mappings.
        /// </summary>
        /// <param name="current">The InputOutputMap of the current node being registered.</param>
        /// <param name="last">The InputOututMap of the previously registered node.</param>
        public delegate void MapActionDelegate(InputOutputMap current, InputOutputMap last);

        /// <summary>
        /// Performs a node registration.
        /// </summary>
        /// <param name="node">The node which is being registered.</param>
        /// <param name="map">The map of the node prior in the chain</param>
        internal void RegisterNode(INode node, MapActionDelegate map)
        {
            node.SetId(Guid.NewGuid());
            var m = new InputOutputMap(node, this);
            map(m, LastNode == null ? null : NodeGraph[LastNode.Id]);
            NodeGraph[node.Id] = m;
            LastNode = node;
        }

        /// <summary>
        /// Registers a subprocess to this process.
        /// </summary>
        /// <param name="attachTo">The node with output which to attach the subprocess to.</param>
        /// <param name="name">The name of the subprocess.  Default uses the name of the current process and appends '(subprocess #)'.</param>
        /// <returns>The new EtlProcessBuilder representing the subprocess.</returns>
        internal EtlProcessBuilder RegisterSubProcess(INode attachTo, string name = null)
        {
            var builder = new EtlProcessBuilder(_settings)
            {
                FirstNode = attachTo,
                LastNode = attachTo,
                Name = name ?? $"{Name} (subprocess {SubProcesses.Count + 1})"
            };
            builder.NodeGraph[attachTo.Id] = NodeGraph[attachTo.Id];

            builder.Log.Debug($"Created new EtlProcessBuilder '{builder.Name}' for subprocess attached to {attachTo}");
            SubProcesses.Add(builder.Id, builder);

            return builder;
        }

        /// <summary>
        /// Retrieves the InputOutputMap for the given node.
        /// </summary>
        /// <param name="node">The node to retrieve the InputOutputMap for.</param>
        /// <returns>The InputOutputMap for the given node.</returns>
        public InputOutputMap GetIoMapForNode(INode node) => NodeGraph[node.Id];

        public EtlProcess Build()
        {
            var process = new EtlProcess(_settings, ProcessContext);

            var method = typeof(EtlProcess).GetMethod("AttachInputToOutput");

            AttachToTargets(FirstNode);

            void AttachToTargets(INode node)
            {
                if (!(node is INodeWithOutput))
                    return;

                var nodeMap = GetIoMapForNode(node);
                
                var outputType = node.GetType().GetInterface(typeof(INodeWithOutput<>).FullName).GenericTypeArguments[0];
                var invocable = method.MakeGenericMethod(outputType);

                foreach (var target in nodeMap.TargetNodes)
                {
                    invocable.Invoke(process, new object[] { node, target });
                    AttachToTargets(target);
                }
            }

            return process;
        }
    }
}