using System;

namespace EtlLib.Pipeline
{
    public interface IDoWhileEtlOperationContext
    {
        IEtlPipeline While(Func<EtlPipelineContext, bool> predicate);
    }

    public class DoWhileEtlOperationContext : IDoWhileEtlOperationContext
    {
        private readonly IEtlPipeline _parentPipeline;
        private readonly IEtlOperationCollection _operationCollection;

        public DoWhileEtlOperationContext(IEtlPipeline parentPipeline, IEtlOperationCollection operationCollection)
        {
            _parentPipeline = parentPipeline;
            _operationCollection = operationCollection;
        }

        public IEtlPipeline While(Func<EtlPipelineContext, bool> predicate)
        {
            _parentPipeline.Run(ctx => new DoWhileEtlOperation(predicate, _operationCollection));
            return _parentPipeline;
        }
    }
}