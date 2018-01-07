namespace EtlLib.Data
{
    public interface IHasFilePath : IFreezable
    {
        string FilePath { get; }
    }
}