using EtlLib.Data;

namespace EtlLib.Support
{
    public interface IEmitter<in T> 
        where T : class, INodeOutput<T>, new()
    {
        void Emit(T item);
        void SignalEnd();
    }
}