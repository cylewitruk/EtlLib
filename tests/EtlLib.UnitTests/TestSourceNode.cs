using System;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Pipeline;
using EtlLib.Support;

namespace EtlLib.UnitTests
{
    public class TestSourceNode : ISourceNode<Row>
    {
        private readonly Action<EtlPipelineContext, IEmitter<Row>> _action;

        public Guid Id { get; private set; }
        public INodeWaiter Waiter { get; private set; }
        public IErrorHandler ErrorHandler { get; private set; }
        public Type OutputType => typeof(Row);
        public IEmitter<Row> TypedEmitter { get; private set; }
        public IEmitter Emitter => TypedEmitter;

        public TestSourceNode(Action<EtlPipelineContext, IEmitter<Row>> action)
        {
            _action = action;
            Id = Guid.NewGuid();
            Waiter = NoWaitNodeWaiter.Instance;
            TypedEmitter = new TestEmitter<Row>();
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

        public ISourceNode<Row> SetEmitter(IEmitter<Row> emitter)
        {
            TypedEmitter = emitter;
            return this;
        }

        public void Execute(EtlPipelineContext context)
        {
            _action(context, TypedEmitter);
        }
    }
}