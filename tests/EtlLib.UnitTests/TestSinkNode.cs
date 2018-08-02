using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Pipeline;
using EtlLib.Support;

namespace EtlLib.UnitTests
{
    public class TestSinkNode : ISinkNode<Row>
    {
        private readonly List<Row> _receivedItems;

        public IReadOnlyList<Row> ReceivedItems => _receivedItems;
        public Action BeforeExecute { get; set; }
        public Action AfterExecute { get; set; }

        public Guid Id { get; private set; }
        public INodeWaiter Waiter { get; private set; }
        public IErrorHandler ErrorHandler { get; private set; }
        public IEnumerable<Row> Input { get; private set; }
        

        public TestSinkNode()
        {
            _receivedItems = new List<Row>();

            Id = Guid.NewGuid();
            Waiter = NoWaitNodeWaiter.Instance;
            ErrorHandler = new TestErrorHandler();
        }

        public INode SetId(Guid id)
        {
            Id = id;
            return this;
        }

        public INode SetWaiter(INodeWaiter waiter)
        {
            Waiter = waiter;
            return this;
        }

        public INode SetErrorHandler(IErrorHandler errorHandler)
        {
            ErrorHandler = errorHandler;
            return this;
        }

        public ISinkNode<Row> SetInput(IEnumerable<Row> input)
        {
            Input = input;
            return this;
        }

        public void Execute(EtlPipelineContext context)
        {
            BeforeExecute?.Invoke();
            Waiter?.Wait();

            foreach (var item in Input)
            {
                _receivedItems.Add(item);
            }

            AfterExecute?.Invoke();
        }
    }
}