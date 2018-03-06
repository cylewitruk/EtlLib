using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class PipelineAbortException : Exception
    {
        private readonly List<EtlOperationError> _errors = new List<EtlOperationError>();

        public IEnumerable<EtlOperationError> Errors => _errors;
        public IEtlOperation Operation { get; }

        public PipelineAbortException(IEtlOperation operation, string message) : base(message)
        {
            Operation = operation;
        }

        public PipelineAbortException(IEtlOperation operation, EtlOperationError error)
            : this(operation, "The ETL pipeline is aborting.")
        {
            _errors.Add(error);
        }
        public PipelineAbortException(IEtlOperation operation, EtlOperationError error, string message)
            : base(message)
        {
            Operation = operation;
            _errors.Add(error);
        }

        public PipelineAbortException(IEtlOperation operation, IEnumerable<EtlOperationError> errors)
            : this(operation, "The ETL pipeline is aborting.")
        {
            _errors.AddRange(errors);
        }

        public PipelineAbortException(IEtlOperation operation, IEnumerable<EtlOperationError> errors, string message)
            : base(message)
        {
            Operation = operation;
            _errors.AddRange(errors);
        }
    }
}