namespace EtlLib.Data
{
    public interface IFreezable
    {
        bool IsFrozen { get; }
        void Freeze();
    }
}