using EtlLib.Logging;

namespace EtlLib
{
    public static class EtlLibConfig
    {
        public static ILoggingAdapter LoggingAdapter { get; set; }
        public static bool EnableDebug { get; set; }

        static EtlLibConfig()
        {
            LoggingAdapter = NullLoggerAdapter.Instance;
            EnableDebug = false;
        }
    }
}