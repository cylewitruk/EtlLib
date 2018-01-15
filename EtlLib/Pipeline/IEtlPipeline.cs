using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        PipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, EtlProcessSettings> settings,
            Action<EtlPipelineContext, IEtlProcessBuilder> builder);

        IEtlPipeline Run(IExecutableNode executable);
        IEtlPipeline Run(Func<EtlPipelineContext, IExecutableNode> ctx);
        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IExecutableNode>> ctx);
    }
}