using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Support;

namespace EtlLib.Pipeline
{
    public class EtlPipelineContext
    {
        private readonly DbConnectionFactory _dbConnectionFactory;

        public IDictionary<string, object> State { get; }
        public ObjectPoolContainer ObjectPool { get; }
        public IEtlPipelineConfig Config { get; }
        public IDbConnectionFactory DbConnectionFactory => _dbConnectionFactory;
        public IDbConnectionRegistrar DbConnections => _dbConnectionFactory;

        internal IDictionary<INode, Exception> Errors { get; }

        public EtlPipelineContext(IEtlPipelineConfig config)
        {
            State = new ConcurrentDictionary<string, object>();
            Errors = new ConcurrentDictionary<INode, Exception>();
            ObjectPool = new ObjectPoolContainer();
            Config = config;
            _dbConnectionFactory = new DbConnectionFactory();
        }

        public EtlPipelineContext() : this(new EtlPipelineConfig()) { }

        public ILogger GetLogger(string name)
        {
            return EtlLibConfig.LoggingAdapter.CreateLogger(name);
        }

        public IDbConnection CreateNamedDbConnection(string name)
        {
            return _dbConnectionFactory.CreateNamedConnection(name);
        }
    }
}
