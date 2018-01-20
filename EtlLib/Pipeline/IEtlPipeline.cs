using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        EtlPipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, IEtlProcessBuilder> builder);

        IEtlPipeline Run(IEtlOperation executable);
        IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx);
        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlOperation>> ctx);
    }
}