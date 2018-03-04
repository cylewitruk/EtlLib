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
            var log = context.GetLogger(GetType().Name);

            if (!_predicate(context))
            {
                log.Info($"Predicate evaluated to false for running of '{Name}', skipping.");
                return new EtlOperationResult(true);
            }

            log.Info($"Predicate evaluated to true for running of '{Name}', executing.");
            return _operation.Execute(context);
        }
    }
}