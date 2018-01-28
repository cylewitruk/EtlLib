using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Support;

namespace EtlLib.Pipeline
{
    public class EtlPipelineContext
    {
        public IDictionary<string, object> State { get; }
        public ObjectPoolContainer ObjectPool { get; }
        public IEtlPipelineConfig Config { get; }

        internal IDictionary<INode, Exception> Errors { get; }

        public EtlPipelineContext(IEtlPipelineConfig config)
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            ObjectPool = new ObjectPoolContainer();
            Config = config;
        }

        public EtlPipelineContext() : this(new EtlPipelineConfig()) { }

        public ILogger GetLogger(string name)
        {
            return EtlLibConfig.LoggingAdapter.CreateLogger(name);
        }
    }
}
