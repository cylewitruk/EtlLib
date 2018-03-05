using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class RunIfTests
    {
        [Fact]
        public void Pipeline_if_clause_runs_contents_if_predicate_returns_true()
        {   
            var wasRun1 = false;
            var wasRun2 = false;
            var wasRunBefore = false;
            var wasRunAfter = false;

            var context = new EtlPipelineContext();

            var pipeline = EtlPipeline.Create(settings => settings
                .UseExistingContext(context)
                .Named("Run If Test"))
                .Run(ctx => new ActionEtlOperation(ctx2 => 
                {
                    wasRunBefore = true;
                    return true;
                }))
                .If(ctx => (string) ctx.State["hello"] == "world", p =>
                {
                    p
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            wasRun1 = true;
                            return true;
                        }).Named("If 1"))
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            wasRun2 = true;
                            return true;
                        }).Named("If 2"));
                })
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    wasRunAfter = true;
                    return true;
                }));

            context.State["hello"] = "world";

            pipeline.Execute();

            wasRunBefore.Should().BeTrue();
            wasRun1.Should().BeTrue();
            wasRun2.Should().BeTrue();
            wasRunAfter.Should().BeTrue();
        }

        [Fact]
        public void Pipeline_if_clause_does_not_run_contents_if_predicate_returns_false()
        {
            var wasRun1 = false;
            var wasRun2 = false;
            var wasRunBefore = false;
            var wasRunAfter = false;

            var context = new EtlPipelineContext();

            var pipeline = EtlPipeline.Create(settings => settings
                    .UseExistingContext(context)
                    .Named("Run If Test"))
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    wasRunBefore = true;
                    return true;
                }))
                .If(ctx => (string)ctx.State["hello"] == "not world", p =>
                {
                    p
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            wasRun1 = true;
                            return true;
                        }).Named("If 1"))
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            wasRun2 = true;
                            return true;
                        }).Named("If 2"));
                })
                .Run(ctx => new ActionEtlOperation(ctx2 =>
                {
                    wasRunAfter = true;
                    return true;
                }));

            context.State["hello"] = "world";

            pipeline.Execute();

            wasRunBefore.Should().BeTrue();
            wasRun1.Should().BeFalse();
            wasRun2.Should().BeFalse();
            wasRunAfter.Should().BeTrue();
        }

        [Fact]
        public void Pipeline_runif_does_not_run_operation_if_predicate_returns_false()
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
        public void Pipeline_runif_runs_operation_if_predicate_returns_true()
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