using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Support;

namespace EtlLib.Pipeline
{
    public class EtlPipelineContext
    {
        public dynamic Test { get; }
        public IDictionary<string, object> State { get; }
        public ObjectPoolContainer ObjectPool { get; }
        public IDictionary<string, string> Config { get; }

        internal IDictionary<INode, Exception> Errors { get; }

        public EtlPipelineContext()
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            ObjectPool = new ObjectPoolContainer();
            Config = new ConcurrentDictionary<string, string>();
            Test = new ExpandoObject();
        }

        public ILogger GetLogger(string name)
        {
            return EtlLibConfig.LoggingAdapter.CreateLogger(name);
        }
    }
}
