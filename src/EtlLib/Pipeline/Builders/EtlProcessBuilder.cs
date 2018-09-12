using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Pipeline.Operations;
using EtlLib.Support;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessBuilder
    {
        Guid Id { get; }
        string Name { get; }

        IEtlProcessBuilder Named(string name);
        IEtlProcessBuilder ThrowOnError(bool throwOnError);
        

        IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlPipelineContext, ISourceNode<TOut>> ctx)
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
        private bool _throwOnError = false;

        private IInputOutputAdapter _last;
        private readonly List<IInputOutputAdapter> _ioAdapters;

        private EtlProcessBuilder(EtlPipelineContext context)
        {
            Id = Guid.NewGuid();
            Name = "Unnamed (" + Id + ")";
            Context = context;
            Log = EtlLibConfig.LoggingAdapter.CreateLogger("EtlLib.EtlProcessBuilder");

            _ioAdapters = new List<IInputOutputAdapter>();
        }

        public static IEtlProcessBuilder Create()
        {
            var builder = new EtlProcessBuilder(new EtlPipelineContext());
            builder.Log.Debug($"Created new EtlProcessBuilder '{builder.Name}'");
            return builder;
        }

        public static IEtlProcessBuilder Create(EtlPipelineContext context)
        {
            var builder = new EtlProcessBuilder(context);
            builder.Log.Debug($"Created new EtlProcessBuilder '{builder.Name}'");
            return builder;
        }

        public IEtlProcessBuilder Named(string name)
        {
            Name = name;
            return this;
        }

        public IOutputNodeBuilderContext<TOut> Input<TOut>(Func<EtlPipelineContext, ISourceNode<TOut>> ctx) 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(Context);

            RegisterOutputNode(node);

            return new OutputNodeBuilderContext<TOut>(this, node);
        }

        public void AttachNodeToOutput<TIn>(ISinkNode<TIn> node)
            where TIn : class, INodeOutput<TIn>, new()
        {
            node.SetId(Guid.NewGuid());

            ((IInputOutputAdapter<TIn>)_last).AttachConsumer(node);
            Log.Debug($"'{Name}' registered [output from] {_last.SourceNode} as [input to] target -> {node}");
        }

        public void RegisterInputOutputNode<TIn, TOut>(IProcessingNode<TIn, TOut> node)
            where TIn : class, INodeOutput<TIn>, new()
            where TOut : class, INodeOutput<TOut>, new()
        {
            node.SetId(Guid.NewGuid());

            Log.Debug($"'{Name}' registered new continue {node}");

            AttachNodeToOutput(node);
            RegisterOutputNode(node);
        }

        public void RegisterOutputNode<TOut>(ISourceNode<TOut> node)
            where TOut : class, INodeOutput<TOut>, new()
        {
            node.SetId(Guid.NewGuid());

            var ioAdapter = new InputOutputAdapter<TOut>(node);
            _ioAdapters.Add(ioAdapter);
            _last = ioAdapter;

            Log.Debug($"'{Name}' registered new input {node}");
        }

        public IEtlProcessBuilder ThrowOnError(bool throwOnError)
        {
            _throwOnError = throwOnError;
            return this;
        }

        public void ClearLastOutputAdapter()
        {
            _last = null;
        }

        /// <summary>
        /// Builds a new EtlProcess.
        /// </summary>
        /// <returns></returns>
        public IEtlOperationWithNoResult Build()
        {
            var process = new EtlProcess(_ioAdapters.ToArray(), new EtlProcessSettings
            {
                ThrowOnError = _throwOnError,
                Name = Name
            });

            return process;
        }

        public IEtlOperationWithEnumerableResult<TOut> Build<TOut>() 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var process = new EtlProcess<TOut>(_ioAdapters.ToArray(), new EtlProcessSettings
            {
                ThrowOnError = _throwOnError,
                Name = Name
            });

            return process;
        }
    }
}