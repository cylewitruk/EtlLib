using System;

namespace EtlLib.Logging.NLog
{
    public class NLogLogger : ILogger
    {
        private readonly global::NLog.ILogger _logger;

        public NLogLogger(global::NLog.ILogger logger)
        {
            _logger = logger;
        }

        public void Trace(string s)
        {
            _logger.Trace(s);
        }

        public void Debug(string s)
        {
            _logger.Debug(s);
        }

        public void Info(string s)
        {
            _logger.Info(s);
        }

        public void Warn(string s)
        {
            _logger.Warn(s);
        }

        public void Error(string s)
        {
            _logger.Error(s);
        }

        public void Error(string s, Exception e)
        {
            _logger.Error(e, s);
        }
    }
}