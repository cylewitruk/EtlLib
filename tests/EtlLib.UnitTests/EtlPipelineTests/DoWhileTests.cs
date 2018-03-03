using System;
using EtlLib.Pipeline;
using Xunit;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class DoWhileTests
    {
        [Fact]
        public void Do_while_test()
        {
            /*
            EtlPipeline.Create(settings => settings
                .Named("Do-While Pipeline Test"))
                .Do(pipeline =>
                {
                    pipeline
                        .Run(ctx => new DummyEtlOperation())
                        .Run(someEtlProcess)
                        .Run(ctx => )
                })
                .While(ctx => ctx.State["remaining_count"] > 100)
                .Execute();*/
            throw new NotImplementedException();
        }
    }
}