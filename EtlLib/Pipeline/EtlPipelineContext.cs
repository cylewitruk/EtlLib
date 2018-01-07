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
        public ILogger Log { get; private set; }

        internal IDictionary<INode, Exception> Errors { get; }
        internal void SetLogger(ILogger log) => Log = log;

        public EtlPipelineContext(ILogger logger)
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            Log = logger;
        }
    }
}
