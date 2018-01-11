using System;
using EtlLib.Logging;

namespace EtlLib.Pipeline
{
    public class EtlPipelineSettings
    {
        public string Name { get; set; }
        public ILoggingAdapter LoggingAdapter { get; set; }
        public Action<EtlPipelineContext> ContextInitializer { get; set; }

        public EtlPipelineSettings()
        {
            LoggingAdapter = new NullLoggerAdapter();
            ContextInitializer = context => { };
        }

        public EtlPipelineSettings WithLoggingAdapter(ILoggingAdapter adapter)
        {
            LoggingAdapter = adapter;
            return this;
        }

        public EtlPipelineSettings Named(string name)
        {
            Name = name;
            return this;
        }

        public EtlPipelineSettings WithContextInitializer(Action<EtlPipelineContext> ctx)
        {
            var context = new EtlPipelineContext(LoggingAdapter);
            ctx(context);
            return this;
        }
    }
}