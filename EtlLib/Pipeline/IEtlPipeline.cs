using System;
using EtlLib.Logging;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        PipelineResult Execute();

        IEtlPipeline Run(Action<EtlProcessSettings> settings, Action<IEtlProcessBuilder> builder);
        IEtlPipeline Run(IExecutable executable);
        IEtlPipeline RunParallel(params IExecutable[] executables);
    }
}