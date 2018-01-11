namespace EtlLib.Pipeline
{
    public interface IExecutable
    {
        string Name { get; }
        void Execute();
    }
}