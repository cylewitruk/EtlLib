using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        PipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, EtlProcessSettings> settings,
            Action<EtlPipelineContext, IEtlProcessBuilder> builder);

        IEtlPipeline Run(IEtlPipelineOperation executable);
        IEtlPipeline Run(Func<EtlPipelineContext, IEtlPipelineOperation> ctx);
        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlPipelineOperation>> ctx);
    }
}