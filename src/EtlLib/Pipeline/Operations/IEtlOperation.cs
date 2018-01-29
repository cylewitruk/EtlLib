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
        IEnumerableEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }

    public interface IEtlOperationWithScalarResult<out TOut> : IEtlOperation
    {
        IScalarEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }
}