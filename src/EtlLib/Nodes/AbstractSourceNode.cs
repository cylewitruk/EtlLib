using System;
using EtlLib.Data;
using EtlLib.Support;

namespace EtlLib.Nodes
{
    public abstract class AbstractSourceNode<TOut> : AbstractNode, ISourceNode<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        public IEmitter<TOut> TypedEmitter { get; private set; }
        public IEmitter Emitter => TypedEmitter;
        public Type OutputType => typeof(TOut);

        public ISourceNode<TOut> SetEmitter(IEmitter<TOut> emitter)
        {
            TypedEmitter = emitter;
            return this;
        }

        protected void Emit(TOut item)
        {
            TypedEmitter?.Emit(item);
        }

        protected void SignalEnd()
        {
            TypedEmitter?.SignalEnd();
        }
    }
}
