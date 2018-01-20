using System;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline.Builders
{
    public interface IOutputNodeBuilderContext<TIn> 
        where TIn : class, INodeOutput<TIn>, new()
    {
        IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlPipelineContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();

        IBranchedNodeBuilderContext<TOut> Branch<TOut>(Func<EtlPipelineContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch1,
            Func<EtlPipelineContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch2)
            where TOut : class, INodeOutput<TOut>, new();

        IEtlProcessCompletedBuilderContext Complete(Func<EtlPipelineContext, INodeWithInput<TIn>> ctx);

        IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlPipelineContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new();
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

        public IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlPipelineContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(_parentBuilder.Context);

            _parentBuilder.RegisterNode(node, (current, last) =>
            {
                // ALL OF THIS NEEDS TO BE REWORKED
                current.AddSourceNode((INodeWithOutput)last.ThisNode);
                last.AddTargetNode(node);

                _log.Debug($"'{_parentBuilder.Name}' registered new continue {node}");
                _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {node}");
            });

            return new OutputNodeBuilderContext<TOut>(_parentBuilder, node);
        }

        public IBranchedNodeBuilderContext<TOut> Branch<TOut>(
            Func<EtlPipelineContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch1, 
            Func<EtlPipelineContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch2) 
            where TOut : class, INodeOutput<TOut>, new()
        {
            var subProcess1 = _parentBuilder.RegisterSubProcess(CreatingNode);
            var subProcess2 = _parentBuilder.RegisterSubProcess(CreatingNode);

            // Looks like we need to create subprocesses here
            var output1 = branch1(_parentBuilder.Context, new OutputNodeBuilderContext<TIn>(subProcess1, CreatingNode));
            var output2 = branch2(_parentBuilder.Context, new OutputNodeBuilderContext<TIn>(subProcess2, CreatingNode));

            return new BranchedNodeBuilderContext<TOut>(_parentBuilder, subProcess1, subProcess2, CreatingNode);
        }

        public IEtlProcessCompletedBuilderContext Complete(Func<EtlPipelineContext, INodeWithInput<TIn>> ctx)
        {
            var node = ctx(_parentBuilder.Context);

            _parentBuilder.RegisterNode(node, (current, last) =>
            {
                current.AddSourceNode((INodeWithOutput)last.ThisNode);
                last.AddTargetNode(node);

                _log.Debug($"'{_parentBuilder.Name}' registered new completion without result {node}");
                _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {node}");
            });

            return new EtlProcessCompletedBuilderContext(_parentBuilder);
        }

        public IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlPipelineContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, INodeOutput<TOut>, new()
        {
            var node = ctx(_parentBuilder.Context);

            _parentBuilder.RegisterNode(node, (current, last) =>
            {
                current.AddSourceNode((INodeWithOutput) last.ThisNode);
                last.AddTargetNode(node);

                _log.Debug($"'{_parentBuilder.Name}' registered new completion with result {node}");
                _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {node}");
            });

            var resultCollectionNode = new GenericResultCollectionNode<TOut>();
            _parentBuilder.RegisterNode(resultCollectionNode, (current, last) =>
                {
                    current.AddSourceNode((INodeWithOutput) last.ThisNode);
                    last.AddTargetNode(resultCollectionNode);

                    _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {resultCollectionNode}");
                });

            return new EtlProcessCompletedWithResultBuilderContext<TOut>(_parentBuilder, resultCollectionNode.Result);
        }
    }
}