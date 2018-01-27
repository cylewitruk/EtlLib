using EtlLib.Data;

namespace EtlLib.Support
{
    public interface IEmitter
    {
        void SignalEnd();
    }

    public interface IEmitter<in T>  : IEmitter
        where T : class, INodeOutput<T>, new()
    {
        void Emit(T item);
    }
}