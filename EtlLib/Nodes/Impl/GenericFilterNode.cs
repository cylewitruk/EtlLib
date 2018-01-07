using System;
using System.Threading.Tasks;
using EtlLib.Data;

namespace EtlLib.Nodes.Impl
{
    public class GenericFilterNode<T> : AbstractInputOutputNode<T, T>
        where T : class, IFreezable
    {
        private readonly Func<T, Task<bool>> _predicate;

        public GenericFilterNode(Func<T, Task<bool>> predicate)
        {
            _predicate = predicate;
        }

        public override async Task Execute()
        {
            foreach (var item in Input)
            {
                if (await _predicate(item))
                    Emit(item);
            }

            Emitter.SignalEnd();
        }
    }
}