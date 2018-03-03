using System;
using System.Linq;
using Xunit;
using FluentAssertions;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;
using Microsoft.VisualStudio.TestPlatform.ObjectModel.Client;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class ExceptionThrowingEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Exception _exception;

        public ExceptionThrowingEtlOperation(Exception exception)
        {
            _exception = exception;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            throw _exception;
        }
    }

    public class ActionEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Func<EtlPipelineContext, bool> _action;

        public ActionEtlOperation(Func<EtlPipelineContext, bool> action)
        {
            _action = action;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return new EtlOperationResult(_action(context));
        }
    }

    public class EtlPipelineErrorHandlingTests
    {
        

        [Fact]
        public void OnError_is_called_when_etl_process_node_raises_error()
        {
            false.Should().BeTrue();
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
            false.Should().BeTrue();
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