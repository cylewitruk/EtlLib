using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class EtlPipelineContext
    {
        public IDictionary<string, object> State { get; }
        public ILoggingAdapter LoggingAdapter { get; private set; }
        public ObjectPoolContainer ObjectPool { get; private set; }

        internal IDictionary<INode, Exception> Errors { get; }
        internal void SetLoggingAdapter(ILoggingAdapter loggingAdapter) => LoggingAdapter = loggingAdapter;

        public EtlPipelineContext(ILoggingAdapter loggingAdapter)
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            LoggingAdapter = loggingAdapter;
            ObjectPool = new ObjectPoolContainer();
        }
    }
}
