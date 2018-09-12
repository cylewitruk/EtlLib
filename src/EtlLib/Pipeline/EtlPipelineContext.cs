using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using EtlLib.Logging;
using EtlLib.Pipeline.Operations;
using EtlLib.Support;

namespace EtlLib.Pipeline
{
    public class EtlPipelineContext
    {
        private readonly DbConnectionFactory _dbConnectionFactory;
        private readonly ConcurrentBag<EtlOperationError> _errors;

        public IDictionary<string, object> State { get; }
        public ObjectPoolContainer ObjectPool { get; }
        public IEtlPipelineConfig Config { get; }
        public IDbConnectionFactory DbConnectionFactory => _dbConnectionFactory;
        public IDbConnectionRegistrar DbConnections => _dbConnectionFactory;

        public EtlPipelineContext(IEtlPipelineConfig config)
        {
            State = new ConcurrentDictionary<string, object>();
            ObjectPool = new ObjectPoolContainer();
            Config = config;
            _dbConnectionFactory = new DbConnectionFactory();
            _errors = new ConcurrentBag<EtlOperationError>();
        }

        public EtlPipelineContext() : this(new EtlPipelineConfig()) { }

        public ILogger GetLogger(string name)
        {
            return EtlLibConfig.LoggingAdapter.CreateLogger(name);
        }

        public virtual IDbConnection CreateNamedDbConnection(string name)
        {
            return _dbConnectionFactory.CreateNamedConnection(name);
        }

        internal void ReportError(EtlOperationError error)
        {
            _errors.Add(error);
        }

        internal void ReportErrors(IEnumerable<EtlOperationError> errors)
        {
            foreach (var error in errors)
                _errors.Add(error);
        }

        public IEnumerable<EtlOperationError> GetCurrentErrors()
        {
            return _errors.ToArray();
        }
    }
}
