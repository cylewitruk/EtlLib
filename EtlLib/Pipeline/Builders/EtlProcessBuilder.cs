using System;
using System.Collections.Generic;
using System.Linq;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessBuilder
    {
        Guid Id { get; }
        string Name { get; }

        IEtlProcessBuilder Named(string name);

        IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlPipelineContext, INodeWithOutput<TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();
    }

    /// <summary>
    /// An EtlProcess is designed to encapsulate a process which can execute independently of any other process and can freely stream results through the process's
    /// nodes.
    /// </summary>
    public class EtlProcessBuilder : IEtlProcessBuilder
    {
        public Guid Id { get; }
        internal ILogger Log { get; }
        internal EtlPipelineContext Context { get; }
        public string Name { get; private set; }

        internal Dictionary<Guid, InputOutputMap> NodeGraph { get; }
        internal Dictionary<Guid, EtlProcessBuilder> SubProcesses { get; }
        internal INode FirstNode { get; private set; }
        internal INode LastNode { get; private set; }

        private EtlProcessBuilder()
        {
            Id = Guid.NewGuid();
            Name = "Unnamed (" + Id + ")";
            Context = new EtlPipelineContext();
            NodeGraph = new Dictionary<Guid, InputOutputMap>();
            SubProcesses = new Dictionary<Guid, EtlProcessBuilder>();
            Log = EtlLibConfig.LoggingAdapter.CreateLogger("EtlLib.EtlProcessBuilder");
        }

        public static IEtlProcessBuilder Create()
        {
            var builder = new EtlProcessBuilder();
            builder.Log.Debug($"Created new EtlProcessBuilder '{builder.Name}'");
            return builder;
        }

        public IEtlProcessBuilder Named(string name)
        {
            Name = name;
            return this;
        }

        public IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlPipelineContext, INodeWithOutput<TOut>> ctx) 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(Context);
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
            var builder = new EtlProcessBuilder()
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

        /// <summary>
        /// Builds a new EtlProcess.
        /// </summary>
        /// <returns></returns>
        public EtlProcess Build()
        {
            var process = new EtlProcess();
            process.SetName(Name);

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

        public EtlProcess<TOut> Build<TOut>() 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var process = new EtlProcess<TOut>();
            process.SetName(Name);

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