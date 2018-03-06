using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class EtlPipelineResult : IEtlOperationResult
    {
        private readonly List<EtlOperationError> _errors;

        public bool IsSuccess { get; private set; }
        public IReadOnlyCollection<EtlOperationError> Errors => _errors;
        public bool WasAborted => AbortedOnOperation != null;
        public IEtlOperation AbortedOnOperation { get; private set; }
        public TimeSpan TotalRunTime { get; private set; }

        public EtlPipelineResult()
        {
            IsSuccess = true;
            _errors = new List<EtlOperationError>();
        }

        public EtlPipelineResult AbortedOn(IEtlOperation operation)
        {
            AbortedOnOperation = operation;
            return this;
        }

        public EtlPipelineResult WithErrors(IEnumerable<EtlOperationError> errors)
        {
            _errors.AddRange(errors);
            return this;
        }

        public EtlPipelineResult WithTotalRunTime(TimeSpan runTime)
        {
            TotalRunTime = runTime;
            return this;
        }

        public EtlPipelineResult QuiesceIsSuccess(bool isSuccess)
        {
            if (IsSuccess && !isSuccess)
                IsSuccess = false;

            return this;
        }
    }
}