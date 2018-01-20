﻿namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlOperationWithScalarResult<TOut> : IEtlOperationWithScalarResult<TOut>
    {
        public string Name { get; private set; }
        public EtlPipelineContext Context { get; private set; }

        public IEtlOperation SetName(string name)
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