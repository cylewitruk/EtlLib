using System;
using EtlLib.Logging;

namespace EtlLib.Pipeline.Builders
{
    public class EtlProcessSettings
    {
        public ILoggingAdapter LoggingAdapter { get; set; }
        public string Name { get; set; }
        public Action<EtlProcessContext> ContextInitializer { get; set; }

        public EtlProcessSettings WithLoggingAdapter(ILoggingAdapter adapter)
        {
            LoggingAdapter = adapter;
            return this;
        }

        public EtlProcessSettings Named(string name)
        {
            Name = name;
            return this;
        }

        public EtlProcessSettings WithContextInitializer(Action<EtlProcessContext> initializer)
        {
            ContextInitializer = initializer;
            return this;
        }
    }
}