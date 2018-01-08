using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public abstract class AbstractOutputNode<TOut> : Node, INodeWithOutput<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        public IEmitter<TOut> Emitter { get; private set; }

        public INodeWithOutput<TOut> SetEmitter(IEmitter<TOut> emitter)
        {
            Emitter = emitter;
            return this;
        }

        protected void Emit(TOut item)
        {
            Emitter?.Emit(item);
        }

        protected void SignalEnd()
        {
            Emitter?.SignalEnd();
        }
    }
}
