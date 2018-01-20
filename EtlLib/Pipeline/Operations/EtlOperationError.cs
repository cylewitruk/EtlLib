using System;
using EtlLib.Nodes;

namespace EtlLib.Pipeline.Operations
{
    public class EtlOperationError
    {
        public INode SourceNode { get; }
        public IEtlOperation SourceOperation { get; }
        public Exception Exception { get; }
        public object SourceItem { get; }

        public bool HasSourceItem => SourceItem != null;

        public EtlOperationError(IEtlOperation sourceOperation, Exception exception, object sourceItem)
        {
            SourceOperation = sourceOperation;
            Exception = exception;
            SourceItem = sourceItem;
        }

        public EtlOperationError(IEtlOperation sourceOperation, Exception exception)
            : this(sourceOperation, exception, null)
        {
        }

        public EtlOperationError(IEtlOperation sourceOperation, INode sourceNode, Exception exception)
            : this(sourceOperation, sourceNode, exception, null)
        {
        }

        public EtlOperationError(IEtlOperation sourceOperation, INode sourceNode, Exception exception,
            object sourceItem)
        {
            SourceOperation = sourceOperation;
            SourceNode = sourceNode;
            Exception = exception;
            SourceItem = sourceItem;
        }
    }
}