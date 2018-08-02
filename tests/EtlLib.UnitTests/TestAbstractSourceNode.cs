using System;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Pipeline;
using EtlLib.Support;

namespace EtlLib.UnitTests
{
    public class TestAbstractSourceNode<T> : AbstractSourceNode<T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly Action<EtlPipelineContext, IEmitter<T>> _emit;

        public TestAbstractSourceNode(Action<EtlPipelineContext, IEmitter<T>> ctx)
        {
            _emit = ctx;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            _emit(context, TypedEmitter);
        }
    }
}