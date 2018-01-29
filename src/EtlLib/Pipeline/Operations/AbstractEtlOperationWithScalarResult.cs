namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlOperationWithScalarResult<TOut> : IEtlOperationWithScalarResult<TOut>
    {
        public string Name { get; private set; }

        public IEtlOperation Named(string name)
        {
            Name = name;
            return this;
        }

        public IEtlOperationResult Execute(EtlPipelineContext context) => ExecuteWithResult(context);

        public abstract IScalarEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }
}