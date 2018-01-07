using System;
using System.Threading.Tasks;
using EtlLib.Data;

namespace EtlLib.Nodes.Impl
{
    public class GenericFilterNode<T> : AbstractInputOutputNode<T, T>
        where T : class, IFreezable
    {
        private readonly Func<T, bool> _predicate;

        public GenericFilterNode(Func<T, bool> predicate)
        {
            _predicate = predicate;
        }

        public override void Execute()
        {
            foreach (var item in Input)
            {
                if (_predicate(item))
                    Emit(item);
            }

            Emitter.SignalEnd();
        }
    }
}