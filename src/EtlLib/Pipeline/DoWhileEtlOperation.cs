using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class DoWhileEtlOperation : AbstractEtlOperationWithNoResult, IConditionalLoopEtlOperation
    {
        private readonly IEtlOperationCollection _pipeline;

        public Func<EtlPipelineContext, bool> Predicate { get; }

        public DoWhileEtlOperation(Func<EtlPipelineContext, bool> predicate, IEtlOperationCollection pipeline)
        {
            _pipeline = pipeline;
            Predicate = predicate;
        }

        public IEnumerable<IEtlOperation> GetOperations()
        {
            return _pipeline.GetOperations();
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return new EtlOperationResult(true);
        }
    }
}