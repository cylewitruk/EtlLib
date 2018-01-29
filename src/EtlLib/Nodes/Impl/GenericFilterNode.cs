using System;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericFilterNode<T> : AbstractInputOutputNode<T, T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly Func<T, bool> _predicate;

        public GenericFilterNode(Func<T, bool> predicate)
        {
            _predicate = predicate;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                if (_predicate(item))
                    Emit(item);
                else
                    context.ObjectPool.Return(item);
            }

            TypedEmitter.SignalEnd();
        }
    }
}