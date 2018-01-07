using EtlLib.Logging;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        PipelineResult Execute();

        IEtlPipeline WithLoggingAdapter(ILoggingAdapter adapter);
    }
}