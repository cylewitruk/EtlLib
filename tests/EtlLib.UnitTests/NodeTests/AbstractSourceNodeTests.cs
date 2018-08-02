using System;
using System.Linq;
using EtlLib.Data;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.NodeTests
{
    public class AbstractSourceNodeTests
    {
        [Fact]
        public void AbstractSourceNode_calls_SignalEnd_when_general_error()
        {
            var exception = new Exception("This is a test exception.");

            var context = new EtlPipelineContext();
            var errorHandler = new TestErrorHandler();
            var emitter = new TestEmitter<Row>();

            var node = new TestAbstractSourceNode<Row>((ctx, e) =>
            {
                e.Emit(new Row());
                throw exception;
            });
            node.SetErrorHandler(errorHandler);
            node.SetEmitter(emitter);

            node.Execute(context);
            
            errorHandler.Errors.Count.Should().Be(1);
            errorHandler.Errors.First().Node.Should().Be(node);
            errorHandler.Errors.First().Exception.Should().Be(exception);
            emitter.HasSignalledEnd.Should().BeTrue();
        }
    }
}