namespace EtlLib.Logging
{
    public class NullLoggerAdapter : ILoggingAdapter
    {
        public ILogger CreateLogger(string name)
        {
            return new NullLogger();
        }
    }

    public class NullLogger : ILogger
    {
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