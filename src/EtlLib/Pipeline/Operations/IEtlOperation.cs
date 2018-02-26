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
        new IEtlOperationWithNoResult Named(string name);
    }

    public interface IEtlOperationWithEnumerableResult<out TOut> : IEtlOperation
    {
        new IEtlOperationWithEnumerableResult<TOut> Named(string name);
        IEnumerableEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }

    public interface IEtlOperationWithScalarResult<out TOut> : IEtlOperation
    {
        new IEtlOperationWithScalarResult<TOut> Named(string name);
        IScalarEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }
}