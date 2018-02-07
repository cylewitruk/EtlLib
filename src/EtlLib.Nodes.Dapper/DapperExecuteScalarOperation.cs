using System;
using System.Data;
using Dapper;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Nodes.Dapper
{
    public class DapperExecuteScalarOperation<T> : AbstractEtlOperationWithScalarResult<T>
    {
        private readonly Func<IDbConnection> _createConnection;
        private readonly string _sql;
        private readonly object _param;
        private IsolationLevel _isolationLevel;
        private int? _timeoutInSeconds;
        private DynamicParameters _dynamicParameters;
        private CommandType _commandType;

        public DapperExecuteScalarOperation(Func<IDbConnection> createConnection, string command, object param = null)
        {
            _createConnection = createConnection;
            _sql = command;
            _param = param;
            _isolationLevel = IsolationLevel.ReadCommitted;
            _commandType = CommandType.Text;
        }

        public DapperExecuteScalarOperation<T> WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public DapperExecuteScalarOperation<T> WithTimeout(TimeSpan timeout)
        {
            _timeoutInSeconds = (int)timeout.TotalSeconds;
            return this;
        }

        public DapperExecuteScalarOperation<T> WithDynamicParameters(Action<DynamicParameters> param)
        {
            _dynamicParameters = new DynamicParameters();
            param(_dynamicParameters);
            return this;
        }

        public DapperExecuteScalarOperation<T> WithCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public override IScalarEtlOperationResult<T> ExecuteWithResult(EtlPipelineContext context)
        {
            using (var con = _createConnection())
            using (var trx = con.BeginTransaction(_isolationLevel))
            {
                var result = con.ExecuteScalar<T>(_sql, _dynamicParameters ?? _param, trx, _timeoutInSeconds, _commandType);
                return new ScalarEtlOperationResult<T>(true, result);
            }
        }
    }
}