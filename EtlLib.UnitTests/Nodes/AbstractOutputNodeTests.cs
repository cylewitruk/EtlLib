using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Pipeline;
using EtlLib.Support;
using FluentAssertions;

namespace EtlLib.UnitTests.Nodes
{
    public class AbstractOutputNodeTests
    {
        public void AbstractOutputNode_calls_SignalEnd_when_general_error()
        {
            var context = new EtlPipelineContext();
            var errorHandler = new TestErrorHandler();
            var nodeStatistics = new NodeStatistics();
            var emitter = new TestEmitter<Row>();

            var node = new TestAbstractOutputNode<Row>((ctx, e) =>
            {
                e.Emit(new Row());
                throw new Exception("This is a test exception.");
            });
            node.SetErrorHandler(errorHandler);
            node.SetEmitter(emitter);
            nodeStatistics.RegisterNode(node);

            node.Execute(context);

            nodeStatistics.TotalWrites.Should().Be(1);
            nodeStatistics.TotalErrors.Should().Be(1);
            errorHandler.Errors.Count.Should().Be(1);
            emitter.HasSignalledEnd.Should().BeTrue();
        }

        
    }
}