using System;
using System.Collections.Generic;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public interface IEtlPipelineOperationResult
    {
        bool IsSuccess { get; }
        IReadOnlyCollection<EtlPipelineOperationError> Errors { get; }
    }

    public class EtlPipelineOperationResult : IEtlPipelineOperationResult
    {
        private readonly List<EtlPipelineOperationError> _errors;

        public bool IsSuccess { get; }
        public IReadOnlyCollection<EtlPipelineOperationError> Errors => _errors;

        public EtlPipelineOperationResult(bool isSuccess)
        {
            IsSuccess = isSuccess;
            _errors = new List<EtlPipelineOperationError>();
        }

        public EtlPipelineOperationResult WithErrors(IEnumerable<EtlPipelineOperationError> errors)
        {
            _errors.AddRange(errors);
            return this;
        }

        public EtlPipelineOperationResult WithError(EtlPipelineOperationError error)
        {
            _errors.Add(error);
            return this;
        }

        public EtlPipelineOperationResult WithError(IEtlPipelineOperation sourceOperation, Exception exception,
            object sourceItem)
        {
            _errors.Add(new EtlPipelineOperationError(sourceOperation, exception, sourceItem));
            return this;
        }

        public EtlPipelineOperationResult WithError(IEtlPipelineOperation sourceOperation, Exception exception)
        {
            _errors.Add(new EtlPipelineOperationError(sourceOperation, exception));
            return this;
        }
    }

    public class EtlPipelineOperationError
    {
        public INode SourceNode { get; }
        public IEtlPipelineOperation SourceOperation { get; }
        public Exception Exception { get; }
        public object SourceItem { get; }

        public bool HasSourceItem => SourceItem != null;

        public EtlPipelineOperationError(IEtlPipelineOperation sourceOperation, Exception exception, object sourceItem)
        {
            SourceOperation = sourceOperation;
            Exception = exception;
            SourceItem = sourceItem;
        }

        public EtlPipelineOperationError(IEtlPipelineOperation sourceOperation, Exception exception)
            : this(sourceOperation, exception, null)
        {
        }

        public EtlPipelineOperationError(IEtlPipelineOperation sourceOperation, INode sourceNode, Exception exception)
            : this(sourceOperation, sourceNode, exception, null)
        {
        }

        public EtlPipelineOperationError(IEtlPipelineOperation sourceOperation, INode sourceNode, Exception exception,
            object sourceItem)
        {
            SourceOperation = sourceOperation;
            SourceNode = sourceNode;
            Exception = exception;
            SourceItem = sourceItem;
        }
    }
}