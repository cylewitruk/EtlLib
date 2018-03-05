using System;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class PipelineAbortException : Exception
    {
        public EtlOperationError OperationError { get; }

        public PipelineAbortException(string message) : base(message)
        {
        }

        public PipelineAbortException(EtlOperationError error)
            : this("The ETL pipeline is aborting.")
        {
            OperationError = error;
        }
        public PipelineAbortException(EtlOperationError error, string message)
            : base(message)
        {
            OperationError = error;
        }
    }
}