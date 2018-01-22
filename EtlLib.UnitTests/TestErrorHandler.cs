using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Support;

namespace EtlLib.UnitTests
{
    public class TestErrorHandler : IErrorHandler
    {
        private readonly ConcurrentBag<NodeError> _errors;

        public IReadOnlyCollection<NodeError> Errors => _errors;

        public TestErrorHandler()
        {
            _errors = new ConcurrentBag<NodeError>();
        }

        public void RaiseError(INode node, Exception e)
        {
            _errors.Add(new NodeError(node, e));
        }

        public void RaiseError(INode node, Exception e, INodeOutput item)
        {
            _errors.Add(new NodeError(node, e, item));
        }

        public class NodeError
        {
            public INode Node { get; }
            public Exception Exception { get; }
            public INodeOutput Item { get; }

            public NodeError(INode node, Exception e, INodeOutput item)
            {
                Node = node;
                Exception = e;
                Item = item;
            }

            public NodeError(INode node, Exception e) : this(node, e, null)
            {
            }
        }
    }
}