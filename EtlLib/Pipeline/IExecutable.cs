namespace EtlLib.Pipeline
{
    public interface IExecutableNode
    {
        string Name { get; }
        void Execute();
    }
}