using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class EtlProcessContext
    {
        public IDictionary<string, object> State { get; }
        public ILoggingAdapter LoggingAdapter { get; }

        internal IDictionary<INode, Exception> Errors { get; }

        public EtlProcessContext(ILoggingAdapter loggingAdapter)
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            LoggingAdapter = loggingAdapter;
        }
    }
}
