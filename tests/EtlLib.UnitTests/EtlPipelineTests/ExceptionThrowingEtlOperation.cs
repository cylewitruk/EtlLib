using System;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class ExceptionThrowingEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Exception _exception;

        public ExceptionThrowingEtlOperation(Exception exception)
        {
            _exception = exception;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            throw _exception;
        }
    }
}