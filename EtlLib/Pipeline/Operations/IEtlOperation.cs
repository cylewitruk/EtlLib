namespace EtlLib.Pipeline.Operations
{
    public interface IEtlOperation
    {
        string Name { get; }

        IEtlOperation Named(string name);
        IEtlOperationResult Execute(EtlPipelineContext context);
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