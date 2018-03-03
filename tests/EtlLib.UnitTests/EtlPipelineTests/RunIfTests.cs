using System;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class RunIfTests
    {
        [Fact]
        public void Pipeline_addition_if_test()
        {/*
            var wasRun = false;

            EtlPipeline.Create(settings => settings
                .Named("Run If Test"))
                .If(ctx => ctx.State["hello"] == "world", pipeline =>
                {
                    pipeline
                        .Run(ctx => new ActionEtlOperation(context =>
                        {
                            wasRun = true;
                            return true;
                        }));
                });*/
            throw new NotImplementedException();
        }

        [Fact]
        public void Pipeline_runif_does_not_run_if_predicate_returns_false()
        {
            var wasRun = false;

            var context = new EtlPipelineContext();
            context.State["hello"] = "not world";

            EtlPipeline.Create(settings => settings
                .UseExistingContext(context)
                .Named("Runif Test"))
                .RunIf(ctx => (string) ctx.State["hello"] == "world",
                    ctx => new ActionEtlOperation(ctx2 =>
                    {
                        wasRun = true;
                        return true;
                    })
                )
                .Execute();

            wasRun.Should().BeFalse();
        }

        [Fact]
        public void Pipeline_runif_runs_if_predicate_returns_true()
        {
            var wasRun = false;

            var context = new EtlPipelineContext();

            var pipeline = EtlPipeline.Create(settings => settings
                    .UseExistingContext(context)
                    .Named("Runif Test"))
                .RunIf(ctx => (string) ctx.State["hello"] == "world",
                    ctx => new ActionEtlOperation(ctx2 =>
                    {
                        wasRun = true;
                        return true;
                    })
                );

            context.State["hello"] = "world";

            pipeline.Execute();

            wasRun.Should().BeTrue();
        }
    }
}