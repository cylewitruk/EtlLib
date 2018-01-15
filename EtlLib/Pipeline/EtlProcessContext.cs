using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Logging;

namespace EtlLib.Pipeline
{
    public class EtlProcessContext<TState>
        where TState : class, new()
    {
        public TState State { get; }
        public IDictionary<string, object> StateDict { get; }
        public ILoggingAdapter LoggingAdapter { get; }
        public EtlPipelineContext PipelineContext { get; }
        public ObjectPoolContainer ObjectPool => PipelineContext.ObjectPool;

        public EtlProcessContext(ILoggingAdapter loggingAdapter)
        {
            StateDict = new ConcurrentDictionary<string, object>();
            LoggingAdapter = loggingAdapter;
            State = new TState();
            PipelineContext = new EtlPipelineContext(loggingAdapter);
        }
    }

    public class EtlProcessContext : EtlProcessContext<object>
    {
        public EtlProcessContext(ILoggingAdapter loggingAdapter)
            : base(loggingAdapter)
        {
        }
    }
}
