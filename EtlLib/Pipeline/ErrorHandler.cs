using System;
using EtlLib.Data;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public interface IErrorHandler
    {
        void RaiseError(INode node, Exception e);
        void RaiseError(INode node, Exception e, INodeOutput item);
    }

    public class ErrorHandler : IErrorHandler
    {
        public Action<INode, Exception, INodeOutput> OnItemErrorFn { get; set; }
        public Action<INode, Exception> OnErrorFn { get; set; }

        public ErrorHandler()
        {
            OnItemErrorFn = null;
            OnErrorFn = null;
        }

        public void RaiseError(INode node, Exception e)
        {
            OnErrorFn?.Invoke(node, e);
        }

        public void RaiseError(INode node, Exception e, INodeOutput item)
        {
            OnItemErrorFn?.Invoke(node, e, item);
        }
    }
}