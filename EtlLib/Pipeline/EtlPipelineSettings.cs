using System;
using System.Collections.Generic;
using EtlLib.Logging;

namespace EtlLib.Pipeline
{
    public class EtlPipelineSettings
    {
        public string Name { get; set; }
        public ILoggingAdapter LoggingAdapter { get; set; }
        public Action<EtlPipelineContext> ContextInitializer { get; set; }
        public List<ObjectPoolSettings> ObjectPoolRegistrations { get; }

        public EtlPipelineSettings()
        {
            LoggingAdapter = new NullLoggerAdapter();
            ContextInitializer = context => { };
            ObjectPoolRegistrations = new List<ObjectPoolSettings>();
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

        public EtlPipelineSettings RegisterObjectPool<T>(int initialSize = 5000, bool autoGrow = true)
        {
            ObjectPoolRegistrations.Add(new ObjectPoolSettings(typeof(T), initialSize, autoGrow));
            return this;
        }
    }
}