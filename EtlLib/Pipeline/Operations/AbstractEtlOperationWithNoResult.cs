namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlOperationWithNoResult : IEtlOperationWithNoResult
    {
        public string Name { get; private set; }
        public EtlPipelineContext Context { get; private set; }

        public IEtlOperation Named(string name)
        {
            Name = name;
            return this;
        }

        public IEtlOperation SetContext(EtlPipelineContext context)
        {
            Context = context;
            OnContextChanged(context);
            return this;
        }

        public abstract IEtlOperationResult Execute();

        public virtual void OnContextChanged(EtlPipelineContext newContext)
        {
        }
    }
}