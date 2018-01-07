using System;
using System.Threading.Tasks;
using EtlLib.Data;

namespace EtlLib.Nodes.Impl
{
    public class GenericMappingNode<TIn, TOut> : AbstractInputOutputNode<TIn, TOut>
        where TIn : class, IFreezable
        where TOut : class, IFreezable
    {
        private readonly Func<TIn, TOut> _mapFn;

        public GenericMappingNode(Func<TIn, TOut> mapFn)
        {
            _mapFn = mapFn;
        }

        public override void Execute()
        {
            foreach (var item in Input)
                Emit(_mapFn(item));

            Emitter.SignalEnd();
        }
    }
}