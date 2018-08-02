using System;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericMappingNode<TIn, TOut> : AbstractProcessingNode<TIn, TOut>
        where TIn : class, INodeOutput<TIn>, new()
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly Func<TIn, TOut, TOut> _mapFn;

        public GenericMappingNode(Func<TIn, TOut, TOut> mapFn)
        {
            _mapFn = mapFn;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                var newItem = context.ObjectPool.Borrow<TOut>();
                try
                {
                    Emit(_mapFn(item, newItem));
                    context.ObjectPool.Return(item);
                }
                catch (Exception ex)
                {
                    RaiseError(ex, item);
                    context.ObjectPool.Return(newItem);
                }
            }

            TypedEmitter.SignalEnd();
        }
    }
}