using System;
using System.Collections.Generic;
using System.Linq;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using Xunit;
using FluentAssertions;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class EtlPipelineErrorHandlingTests
    {
        

        [Fact]
        public void OnError_is_called_when_etl_process_node_raises_error()
        {
            var exception = new Exception("Whoops!");
            var exceptionThrowingOp = new ExceptionThrowingEtlOperation(exception);
            var errorHandlerCalled = false;

            var input = new List<Row>
            {
                new Row {["number"] = 1},
                new Row {["number"] = 2}
            };

            var inputNode = new TestSourceNode((ctx, emitter) =>
            {
                foreach (var item in input)
                    emitter.Emit(item);
                emitter.SignalEnd();
            });

            var context = new EtlPipelineContext();

            var transformNode = new GenericTransformationNode<Row>((objects, row) => throw exception);

            var process = EtlProcessBuilder.Create(context)
                .Input(ctx => inputNode)
                .Continue(ctx => transformNode)
                .Complete(ctx => new TestSinkNode())
                .Build();

            var pipeline = EtlPipeline.Create(settings => settings
                    .UseExistingContext(context)
                    .OnError((ctx, errors) =>
                    {
                        errorHandlerCalled = true;
                        errors.Length.Should().Be(2);

                        errors[0].Exception.Should().Be(exception);
                        errors[0].SourceOperation.Should().Be(process);
                        errors[0].HasSourceItem.Should().BeTrue();
                        errors[0].SourceNode.Should().Be(transformNode);
                        errors[0].SourceItem.Should().Be(input[1]);

                        errors[1].Exception.Should().Be(exception);
                        errors[1].SourceOperation.Should().Be(process);
                        errors[1].HasSourceItem.Should().BeTrue();
                        errors[1].SourceNode.Should().Be(transformNode);
                        errors[1].SourceItem.Should().Be(input[0]);
                        return true;
                    })
                )
                .Run(ctx => process)
                .Execute();

            errorHandlerCalled.Should().BeTrue();
        }

        [Fact]
        public void OnError_is_called_when_etl_operation_throws_exception()
        {
            var exception = new Exception("Whoops!");
            var exceptionThrowingOp = new ExceptionThrowingEtlOperation(exception);
            var errorHandlerCalled = false;

            var pipeline = EtlPipeline.Create(settings => settings
                .OnError((ctx, errors) =>
                {
                    errorHandlerCalled = true;
                    errors.Count().Should().Be(1);
                    var err = errors.Single();
                    err.Exception.Should().Be(exception);
                    err.SourceOperation.Should().Be(exceptionThrowingOp);
                    err.HasSourceItem.Should().BeFalse();
                    err.SourceNode.Should().BeNull();
                    return true;
                })
            )
            .Run(exceptionThrowingOp)
            .Execute();

            errorHandlerCalled.Should().BeTrue();
        }

        [Fact]
        public void OnError_is_called_when_etl_operation_returns_errors()
        {
            var exception = new Exception("Whoops!");
            var errorReturningOp = new ActionEtlOperation(ctx => false);
            errorReturningOp.WithErrors(new EtlOperationError(errorReturningOp, exception));
            var errorHandlerCalled = false;

            var pipeline = EtlPipeline.Create(settings => settings
                    .OnError((ctx, errors) =>
                    {
                        errorHandlerCalled = true;
                        errors.Count().Should().Be(1);
                        var err = errors.Single();
                        err.Exception.Should().Be(exception);
                        err.SourceOperation.Should().Be(errorReturningOp);
                        err.HasSourceItem.Should().BeFalse();
                        err.SourceNode.Should().BeNull();
                        return true;
                    })
                )
                .Run(errorReturningOp)
                .Execute();

            errorHandlerCalled.Should().BeTrue();
        }

        [Fact]
        public void EtlPipeline_stops_executing_when_OnError_returns_false()
        {
            var exception = new Exception("Whoops!");
            var exceptionThrowingOp = new ExceptionThrowingEtlOperation(exception);
            var operationAfterErrorRun = false;

            var pipeline = EtlPipeline.Create(settings => settings
                    .OnError((ctx, errors) => false)
                )
                .Run(exceptionThrowingOp)
                .Run(ctx => new ActionEtlOperation(context => operationAfterErrorRun = true))
                .Execute();
            
            operationAfterErrorRun.Should().BeFalse();
        }

        [Fact]
        public void EtlPipeline_continues_executing_when_OnError_returns_true()
        {
            var exception = new Exception("Whoops!");
            var exceptionThrowingOp = new ExceptionThrowingEtlOperation(exception);
            var operationAfterErrorRun = false;

            var pipeline = EtlPipeline.Create(settings => settings
                    .OnError((ctx, errors) => true)
                )
                .Run(exceptionThrowingOp)
                .Run(ctx => new ActionEtlOperation(context => operationAfterErrorRun = true))
                .Execute();

            operationAfterErrorRun.Should().BeTrue();
        }

        [Fact]
        public void OnError_stops_etl_pipeline_execution_by_default()
        {
            var exception = new Exception("Whoops!");
            var exceptionThrowingOp = new ExceptionThrowingEtlOperation(exception);
            var operationAfterErrorRun = false;

            var pipeline = EtlPipeline.Create(settings => {})
                .Run(exceptionThrowingOp)
                .Run(ctx => new ActionEtlOperation(context => operationAfterErrorRun = true))
                .Execute();

            operationAfterErrorRun.Should().BeFalse();
        }
    }
}