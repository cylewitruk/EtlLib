namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlOperationWithEnumerableResult<TOut> : IEtlOperationWithEnumerableResult<TOut>
    {
        public string Name { get; private set; }

        public IEtlOperation Named(string name)
        {
            Name = name;
            return this;
        }

        public IEtlOperationResult Execute(EtlPipelineContext context) => ExecuteWithResult(context);

        public abstract IEnumerableEtlOperationResult<TOut> ExecuteWithResult(EtlPipelineContext context);
    }
}