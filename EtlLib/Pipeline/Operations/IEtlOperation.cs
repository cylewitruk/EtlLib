namespace EtlLib.Pipeline.Operations
{
    public interface IEtlOperation
    {
        string Name { get; }
        EtlPipelineContext Context { get; }

        IEtlOperation SetName(string name);
        IEtlOperation SetContext(EtlPipelineContext context);

        IEtlOperationResult Execute();
    }

    public interface IEtlOperationWithNoResult : IEtlOperation
    {
    }

    public interface IEtlOperationWithEnumerableResult<out TOut> : IEtlOperation
    {
    }

    public interface IEtlOperationWithScalarResult<out TOut> : IEtlOperation
    {
    }
}