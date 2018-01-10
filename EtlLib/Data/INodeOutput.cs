namespace EtlLib.Data
{
    public interface INodeOutput : IResettable
    {
        bool IsFrozen { get; }
        void Freeze();
    }

    public interface INodeOutput<in T> : INodeOutput
        where T : class, new()
    {
        void CopyTo(T obj);
    }
}