using System;
using System.Collections.Generic;
using EtlLib.Pipeline;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class DoWhileTests
    {
        //[Fact]
        public void Do_while_test()
        {/*
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
                .While(ctx => ctx.State["remaining_count"] > 1)
                .Execute();*/

            throw new NotImplementedException();
        }
    }
}