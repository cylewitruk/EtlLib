using System;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class RunPipelineWithinPipelineTests
    {
        [Fact]
        public void Pipeline_can_execute_another_pipeline()
        {
            var run1 = false;
            var run2 = false;
            var run3 = false;

            var innerPipeline = EtlPipeline.Create(settings => settings
                .Named("Pipeline 2"))
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    run2 = true;
                    return true;
                }));

            var result = EtlPipeline.Create(settings => settings
                .Named("Pipeline 1"))
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    run1 = true;
                    return true;
                }))
                .Run(ctx => innerPipeline)
                .Run(new ActionEtlOperation(ctx =>
                {
                    run3 = true;
                    return true;
                }))
                .Execute();

            run1.Should().BeTrue();
            run2.Should().BeTrue();
            run3.Should().BeTrue();

            result.Should().NotBeNull();
            result.IsSuccess.Should().BeTrue();
        }

        [Fact]
        public void Pipeline_aborts_when_error_encountered_when_executing_a_nested_pipeline()
        {
            var run1 = false;
            var run2 = false;
            var run3 = false;

            var innerPipeline = EtlPipeline.Create(settings => settings
                .Named("Pipeline 2"))
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    run2 = true;
                    throw new Exception("Uh oh!");
                }));

            var result = EtlPipeline.Create(settings => settings
                    .Named("Pipeline 1"))
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    run1 = true;
                    return true;
                }))
                .Run(ctx => innerPipeline)
                .Run(new ActionEtlOperation(ctx =>
                {
                    run3 = true;
                    return true;
                }))
                .Execute();

            run1.Should().BeTrue();
            run2.Should().BeTrue();
            run3.Should().BeFalse();

            result.Should().NotBeNull();
            result.IsSuccess.Should().BeFalse();
        }
    }
}