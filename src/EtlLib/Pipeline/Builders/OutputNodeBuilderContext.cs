using System;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Nodes.Impl;

namespace EtlLib.Pipeline.Builders
{
    public interface IOutputNodeBuilderContext<TIn> 
        where TIn : class, INodeOutput<TIn>, new()
    {
        IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlPipelineContext, IProcessingNode<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();

        IEtlProcessCompletedBuilderContext Complete(Func<EtlPipelineContext, ISinkNode<TIn>> ctx);

        IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlPipelineContext, IProcessingNode<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();

        IEtlProcessCompletedWithResultBuilderContext<TIn> CompleteWithResult();
    }

    public class OutputNodeBuilderContext<TIn> : IOutputNodeBuilderContext<TIn>
        where TIn : class, INodeOutput<TIn>, new()
    {
        private readonly EtlProcessBuilder _parentBuilder;
        private readonly ILogger _log;

        public INode CreatingNode { get; }

        public OutputNodeBuilderContext(EtlProcessBuilder parentBuilder, INode creatingNode)
        {
            _parentBuilder = parentBuilder;
            _log = parentBuilder.Log;
            CreatingNode = creatingNode;
        }

        public IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlPipelineContext, IProcessingNode<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(_parentBuilder.Context);

            _parentBuilder.RegisterInputOutputNode(node);

            return new OutputNodeBuilderContext<TOut>(_parentBuilder, node);
        }

        public IEtlProcessCompletedBuilderContext Complete(Func<EtlPipelineContext, ISinkNode<TIn>> ctx)
        {
            var node = ctx(_parentBuilder.Context);

            _parentBuilder.AttachNodeToOutput(node);
            _parentBuilder.ClearLastOutputAdapter();

            return new EtlProcessCompletedBuilderContext(_parentBuilder);
        }

        public IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlPipelineContext, IProcessingNode<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(_parentBuilder.Context);

            _log.Debug($"'{_parentBuilder.Name}' registered new completion with result {node}");

            _parentBuilder.RegisterInputOutputNode(node);
            var resultCollectionNode = new GenericResultCollectionNode<TOut>();
            _parentBuilder.AttachNodeToOutput(resultCollectionNode);

            return new EtlProcessCompletedWithResultBuilderContext<TOut>(_parentBuilder, resultCollectionNode.Result);
        }

        public IEtlProcessCompletedWithResultBuilderContext<TIn> CompleteWithResult()
        {
            var resultCollectionNode = new GenericResultCollectionNode<TIn>();
            _parentBuilder.AttachNodeToOutput(resultCollectionNode);

            return new EtlProcessCompletedWithResultBuilderContext<TIn>(_parentBuilder, resultCollectionNode.Result);
        }
    }
}