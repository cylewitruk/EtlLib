namespace EtlLib.Data
{
    public abstract class Frozen : IFreezable
    {
        public bool IsFrozen => true;
        public void Freeze()
        {
        }
    }
}