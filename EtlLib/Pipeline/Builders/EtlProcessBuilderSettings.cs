using EtlLib.Logging;

namespace EtlLib.Pipeline.Builders
{
    public class EtlProcessBuilderSettings
    {
        public ILoggingAdapter LoggingAdapter { get; set; }
        public string Name { get; set; }

        public EtlProcessBuilderSettings WithLoggingAdapter(ILoggingAdapter adapter)
        {
            LoggingAdapter = adapter;
            return this;
        }

        public EtlProcessBuilderSettings Named(string name)
        {
            Name = name;
            return this;
        }
    }
}