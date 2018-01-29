namespace EtlLib.Data
{
    public abstract class Frozen<T>
        where T : class, INodeOutput<T>, new()
    {
        public bool IsFrozen => true;
        public void Freeze()
        {
        }
    }
}