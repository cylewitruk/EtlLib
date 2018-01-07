using EtlLib.Data;

namespace EtlLib.Pipeline
{
    public interface IEmitter<in T> where T : class, IFreezable
    {
        void Emit(T item);
        void SignalEnd();
    }
}