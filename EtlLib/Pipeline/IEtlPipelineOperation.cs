namespace EtlLib.Pipeline
{
    public interface IEtlPipelineOperation
    {
        string Name { get; }
        EtlPipelineContext Context { get; }

        IEtlPipelineOperation SetName(string name);
        IEtlPipelineOperation SetContext(EtlPipelineContext context);

        IEtlPipelineOperationResult Execute();
    }
}