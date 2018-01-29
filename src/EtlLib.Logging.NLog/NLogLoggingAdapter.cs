using NLog;

namespace EtlLib.Logging.NLog
{
    public class NLogLoggingAdapter : ILoggingAdapter
    {
        public ILogger CreateLogger(string name)
        {
            return new NLogLogger(new LogFactory().GetLogger(name));
        }
    }
}