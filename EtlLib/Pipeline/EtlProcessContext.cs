using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class EtlProcessContext<T>
        where T : new()
    {
        public T State { get; }
        public IDictionary<string, object> StateDict { get; }
        public ILoggingAdapter LoggingAdapter { get; }

        internal IDictionary<INode, Exception> Errors { get; }

        public EtlProcessContext(ILoggingAdapter loggingAdapter)
        {
            StateDict = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            LoggingAdapter = loggingAdapter;
            State = new T();
        }
    }

    public class EtlProcessContext : EtlProcessContext<object>
    {
        public EtlProcessContext(ILoggingAdapter loggingAdapter)
            : base(loggingAdapter)
        {
        }
    }
}
