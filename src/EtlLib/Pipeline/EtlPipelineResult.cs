using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class EtlPipelineResult : IEtlOperationResult
    {
        public bool IsSuccess { get; }
        public IReadOnlyCollection<EtlOperationError> Errors { get; }
        public TimeSpan TotalRunTime { get; }

        public EtlPipelineResult(bool isSuccess, IEnumerable<EtlOperationError> errors, TimeSpan totalRunTime)
        {
            IsSuccess = isSuccess;
            Errors = new List<EtlOperationError>(errors);
            TotalRunTime = totalRunTime;
        }
    }
}