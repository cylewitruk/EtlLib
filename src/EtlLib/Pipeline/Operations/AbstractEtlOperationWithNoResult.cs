namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlOperationWithNoResult : IEtlOperationWithNoResult
    {
        public string Name { get; private set; }

        public IEtlOperation Named(string name)
        {
            Name = name;
            return this;
        }

        IEtlOperationWithNoResult IEtlOperationWithNoResult.Named(string name)
        {
            Named(name);
            return this;
        }

        public abstract IEtlOperationResult Execute(EtlPipelineContext context);
    }
}