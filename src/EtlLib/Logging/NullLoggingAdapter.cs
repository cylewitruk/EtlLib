namespace EtlLib.Logging
{
    public class NullLoggerAdapter : ILoggingAdapter
    {
        public static NullLoggerAdapter Instance;

        static NullLoggerAdapter()
        {
            Instance = new NullLoggerAdapter();
        }

        public ILogger CreateLogger(string name)
        {
            return NullLogger.Instance;
        }
    }

    public class NullLogger : ILogger
    {
        public static NullLogger Instance;

        static NullLogger()
        {
            Instance = new NullLogger();
        }

        public void Trace(string s)
        {
        }

        public void Debug(string s)
        {
        }

        public void Info(string s)
        {
        }

        public void Warn(string s)
        {
        }

        public void Error(string s)
        {
        }
    }
}