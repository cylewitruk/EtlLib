namespace EtlLib.Data
{
    public interface INodeOutput<in T> : IResettable
        where T : class, new()
    {
        bool IsFrozen { get; }
        void Freeze();
        
        void CopyTo(T obj);
    }
}