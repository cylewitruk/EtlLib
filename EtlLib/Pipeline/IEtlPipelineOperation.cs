namespace EtlLib.Pipeline
{
    public interface IEtlPipelineOperation
    {
        string Name { get; }
        IEtlPipelineOperationResult Execute();
    }
}