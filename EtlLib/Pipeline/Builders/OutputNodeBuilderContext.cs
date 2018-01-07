using System;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline.Builders
{
    public interface IOutputNodeBuilderContext<TIn> 
        where TIn : class, IFreezable
    {
        IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlProcessContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, IFreezable;

        IBranchedNodeBuilderContext<TOut> Branch<TOut>(Func<EtlProcessContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch1,
            Func<EtlProcessContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch2)
            where TOut : class, IFreezable;

        IEtlProcessCompletedBuilderContext Complete(Func<EtlProcessContext, INodeWithInput<TIn>> ctx);

        IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlProcessContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, IFreezable;
    }

    public class OutputNodeBuilderContext<TIn> : IOutputNodeBuilderContext<TIn>
        where TIn : class, IFreezable
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

        public IOutputNodeBuilderContext<TOut> Continue<TOut>(Func<EtlProcessContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, IFreezable
        {
            var node = ctx(_parentBuilder.ProcessContext);

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
            Func<EtlProcessContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch1, 
            Func<EtlProcessContext, IOutputNodeBuilderContext<TIn>, IOutputNodeBuilderContext<TOut>> branch2) 
            where TOut : class, IFreezable
        {
            var subProcess1 = _parentBuilder.RegisterSubProcess(CreatingNode);
            var subProcess2 = _parentBuilder.RegisterSubProcess(CreatingNode);

            // Looks like we need to create subprocesses here
            var output1 = branch1(_parentBuilder.ProcessContext, new OutputNodeBuilderContext<TIn>(subProcess1, CreatingNode));
            var output2 = branch2(_parentBuilder.ProcessContext, new OutputNodeBuilderContext<TIn>(subProcess2, CreatingNode));

            return new BranchedNodeBuilderContext<TOut>(_parentBuilder, subProcess1, subProcess2, CreatingNode);
        }

        public IEtlProcessCompletedBuilderContext Complete(Func<EtlProcessContext, INodeWithInput<TIn>> ctx)
        {
            var node = ctx(_parentBuilder.ProcessContext);

            _parentBuilder.RegisterNode(node, (current, last) =>
            {
                current.AddSourceNode((INodeWithOutput)last.ThisNode);
                last.AddTargetNode(node);

                _log.Debug($"'{_parentBuilder.Name}' registered new completion without result {node}");
                _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {node}");
            });

            return new EtlProcessCompletedBuilderContext(_parentBuilder);
        }

        public IEtlProcessCompletedWithResultBuilderContext<TOut> CompleteWithResult<TOut>(Func<EtlProcessContext, INodeWithInputOutput<TIn, TOut>> ctx)
            where TOut : class, IFreezable
        {
            var node = ctx(_parentBuilder.ProcessContext);

            _parentBuilder.RegisterNode(node, (current, last) =>
            {
                current.AddSourceNode((INodeWithOutput)last.ThisNode);
                last.AddTargetNode(node);

                _log.Debug($"'{_parentBuilder.Name}' registered new continue {node}");
                _log.Debug($"'{_parentBuilder.Name}' registered [output from] {last.ThisNode} as [input to] target -> {node}");
            });

            return new EtlProcessCompletedWithResultBuilderContext<TOut>(_parentBuilder);
        }
    }
}