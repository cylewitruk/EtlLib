using EtlLib.Logging;

namespace EtlLib
{
    public static class EtlLibConfig
    {
        public static ILoggingAdapter LoggingAdapter { get; set; }

        static EtlLibConfig()
        {
            LoggingAdapter = NullLoggerAdapter.Instance;
        }
    }
}