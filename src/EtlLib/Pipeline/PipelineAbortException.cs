using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class PipelineAbortException : Exception
    {
        private readonly List<EtlOperationError> _errors = new List<EtlOperationError>();

        public IEnumerable<EtlOperationError> Errors => _errors;

        public PipelineAbortException(string message) : base(message)
        {
        }

        public PipelineAbortException(EtlOperationError error)
            : this("The ETL pipeline is aborting.")
        {
            _errors.Add(error);
        }
        public PipelineAbortException(EtlOperationError error, string message)
            : base(message)
        {
            _errors.Add(error);
        }

        public PipelineAbortException(IEnumerable<EtlOperationError> errors)
            : this("The ETL pipeline is aborting.")
        {
            _errors.AddRange(errors);
        }

        public PipelineAbortException(IEnumerable<EtlOperationError> errors, string message)
            : base(message)
        {
            _errors.AddRange(errors);
        }
    }
}