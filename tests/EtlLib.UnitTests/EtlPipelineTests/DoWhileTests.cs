using System;
using System.Collections.Generic;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class DoWhileTests
    {
        [Fact]
        public void Do_while_general_test()
        {
            var context = new EtlPipelineContext();

            var items = new Queue<string>(new [] {"The", "Quick", "Brown", "Foxed", "Jumps", "Over", "The", "Lazy", "Dog"});
            var iterations = 0;

            var getCountOperation = new ActionEtlOperation(ctx => 
            {
                ctx.State["remaining_count"] = items.Count;
                return true;
            });
            
            EtlPipeline.Create(settings => settings
                .UseExistingContext(context)
                .Named("Do-While Pipeline Test"))
                .Run(getCountOperation)
                .Do(pipeline =>
                {
                    pipeline
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            items.Dequeue();
                            iterations++;
                            return true;
                        }))
                        .Run(getCountOperation);
                })
                .While(ctx => (int)ctx.State["remaining_count"] > 0)
                .Execute();

            iterations.Should().Be(9);
        }

        [Fact]
        public void Do_while_breaks_loop_when_predicate_returns_false()
        {
            var context = new EtlPipelineContext();

            var items = new Queue<string>(new[] { "The", "Quick", "Brown", "Foxed", "Jumps", "Over", "The", "Lazy", "Dog" });
            var iterations = 0;

            var getCountOperation = new ActionEtlOperation(ctx =>
            {
                ctx.State["remaining_count"] = items.Count;
                return true;
            });

            var executedAfter = false;

            EtlPipeline.Create(settings => settings
                    .UseExistingContext(context)
                    .Named("Do-While Pipeline Test"))
                .Run(getCountOperation)
                .Do(pipeline =>
                {
                    pipeline
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            items.Dequeue();
                            iterations++;
                            return true;
                        }))
                        .Run(getCountOperation);
                })
                .While(ctx => (int)ctx.State["remaining_count"] > 5)
                .Run(new ActionEtlOperation(ctx => 
                {
                    executedAfter = true;
                    return true;
                }))
                .Execute();

            iterations.Should().Be(4);
        }

        [Fact]
        public void Do_while_stops_execution_when_loop_returns_error_with_default_error_handling()
        {
            var context = new EtlPipelineContext();

            var items = new Queue<string>(new[] { "The", "Quick", "Brown", "Foxed", "Jumps", "Over", "The", "Lazy", "Dog" });
            var iterations = 0;

            var getCountOperation = new ActionEtlOperation(ctx =>
            {
                ctx.State["remaining_count"] = items.Count;
                return true;
            });

            EtlPipeline.Create(settings => settings
                    .UseExistingContext(context)
                    .Named("Do-While Pipeline Test"))
                .Run(getCountOperation)
                .Do(pipeline =>
                {
                    pipeline
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            items.Dequeue();
                            iterations++;
                            return iterations != 5;
                        }))
                        .Run(getCountOperation);
                })
                .While(ctx => (int)ctx.State["remaining_count"] > 5)
                .Execute();

            iterations.Should().Be(4);
        }

        [Fact]
        public void Do_while_continues_execution_when_loop_returns_error_with_custom_errorhandling()
        {
            var context = new EtlPipelineContext();

            var items = new Queue<string>(new[] { "The", "Quick", "Brown", "Foxed", "Jumps", "Over", "The", "Lazy", "Dog" });
            var iterations = 0;

            var getCountOperation = new ActionEtlOperation(ctx =>
            {
                ctx.State["remaining_count"] = items.Count;
                return true;
            });

            EtlPipeline.Create(settings => settings
                .UseExistingContext(context)
                .Named("Do-While Pipeline Test")
                .OnError((ctx, errors) => true))
                .Run(getCountOperation)
                .Do(pipeline =>
                {
                    pipeline
                        .Run(ctx => new ActionEtlOperation(ctx2 =>
                        {
                            items.Dequeue();
                            iterations++;
                            return iterations != 5;
                        }))
                        .Run(getCountOperation);
                })
                .While(ctx => (int)ctx.State["remaining_count"] > 0)
                .Execute();

            iterations.Should().Be(9);
        }
    }
}