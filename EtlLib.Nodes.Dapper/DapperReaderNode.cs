using System;
using System.Data;
using Dapper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Dapper
{
    public class DapperReaderNode<TOut> : AbstractOutputNode<TOut> 
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly Func<IDbConnection> _createConnection;
        private readonly string _sql;
        private readonly object _param;
        private IsolationLevel _isolationLevel;
        private bool _buffered;
        private int? _timeoutInSeconds;
        private DynamicParameters _dynamicParameters;
        private CommandType _commandType;

        public DapperReaderNode(Func<IDbConnection> createConnection, string command, object param = null)
        {
            _createConnection = createConnection;
            _sql = command;
            _param = param;
            _isolationLevel = IsolationLevel.ReadCommitted;
            _buffered = true;
            _commandType = CommandType.Text;
        }

        public DapperReaderNode<TOut> WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public DapperReaderNode<TOut> WithBuffer(bool bufferResults)
        {
            _buffered = bufferResults;
            return this;
        }

        public DapperReaderNode<TOut> WithTimeout(TimeSpan timeout)
        {
            _timeoutInSeconds = (int)timeout.TotalSeconds;
            return this;
        }

        public DapperReaderNode<TOut> WithDynamicParameters(Action<DynamicParameters> param)
        {
            _dynamicParameters = new DynamicParameters();
            param(_dynamicParameters);
            return this;
        }

        public DapperReaderNode<TOut> WithCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            using (var con = _createConnection())
            using (var trx = con.BeginTransaction(_isolationLevel))
            {
                foreach (var result in con.Query<TOut>(_sql, _dynamicParameters ?? _param, trx, _buffered, _timeoutInSeconds, _commandType))
                {
                    Emit(result);
                }
            }

            SignalEnd(); //TODO: SignalEnd() really could be called by the task instead straight to the IOAdapter when Execute() returns?
        }
    }
}
