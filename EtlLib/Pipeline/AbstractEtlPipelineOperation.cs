namespace EtlLib.Pipeline
{
    public abstract class AbstractEtlPipelineOperation : IEtlPipelineOperation
    {
        public string Name { get; private set; }
        public EtlPipelineContext Context { get; private set; }

        public IEtlPipelineOperation SetName(string name)
        {
            Name = name;
            return this;
        }

        public IEtlPipelineOperation SetContext(EtlPipelineContext context)
        {
            Context = context;
            OnContextChanged(context);
            return this;
        }

        public abstract IEtlPipelineOperationResult Execute();

        public virtual void OnContextChanged(EtlPipelineContext newContext)
        {
        }
    }
}