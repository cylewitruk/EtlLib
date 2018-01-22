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

        public abstract IEtlOperationResult Execute(EtlPipelineContext context);
    }
}