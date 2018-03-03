using System;

namespace EtlLib.Pipeline.Operations
{
    public class ConditionalEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Func<EtlPipelineContext, bool> _predicate;
        private readonly IEtlOperation _operation;

        public ConditionalEtlOperation(Func<EtlPipelineContext, bool> predicate, IEtlOperation operation)
        {
            _predicate = predicate;
            _operation = operation;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return !_predicate(context) 
                ? new EtlOperationResult(true) 
                : _operation.Execute(context);
        }
    }
}