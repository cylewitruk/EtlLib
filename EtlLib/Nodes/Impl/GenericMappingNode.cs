using System;
using EtlLib.Data;

namespace EtlLib.Nodes.Impl
{
    public class GenericMappingNode<TIn, TOut> : AbstractInputOutputNode<TIn, TOut>
        where TIn : class, INodeOutput<TIn>, new()
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly Func<TIn, TOut, TOut> _mapFn;

        public GenericMappingNode(Func<TIn, TOut, TOut> mapFn)
        {
            _mapFn = mapFn;
        }

        public override void Execute()
        {
            foreach (var item in Input)
            {
                var newItem = Context.ObjectPool.Borrow<TOut>();
                Emit(_mapFn(item, newItem));
                Context.ObjectPool.Return(item);
            }

            Emitter.SignalEnd();
        }
    }
}