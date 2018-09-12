using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class SimpleDbOuterJoinNode : AbstractProcessingNode<Row, Row>
    {
        private readonly string _dbQuery;
        private readonly string _leftColumn;
        private readonly string _rightColumn;
        private readonly string _connectionName;
        private readonly Dictionary<string, object> _parameters;
        private IsolationLevel _isolationLevel;
        private CommandType _commandType;

        public SimpleDbOuterJoinNode(string connectionName, string dbQuery, string leftColumn, string rightColumn)
        {
            _connectionName = connectionName;
            _dbQuery = dbQuery;
            _leftColumn = leftColumn;
            _rightColumn = rightColumn;
            _parameters = new Dictionary<string, object>();
            _isolationLevel = IsolationLevel.ReadCommitted;
            _commandType = CommandType.Text;
        }

        public SimpleDbOuterJoinNode WithParameter(string name, object value)
        {
            _parameters[name] = value;
            return this;
        }

        public SimpleDbOuterJoinNode WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public SimpleDbOuterJoinNode WithCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            using (var con = context.CreateNamedDbConnection(_connectionName))
            {
                if (con.State != ConnectionState.Open)
                    con.Open();

       
                foreach (var item in Input)
                {
                    var emitted = false;
                    using (var trx = con.BeginTransaction(_isolationLevel))
                    using (var cmd = con.CreateCommand())
                    {
                        cmd.CommandText = _dbQuery;
                        cmd.CommandType = _commandType;
                        cmd.Transaction = trx;

                        foreach (var param in _parameters)
                        {
                            var p = cmd.CreateParameter();
                            p.ParameterName = param.Key;
                            p.Value = param.Value;

                            cmd.Parameters.Add(p);
                        }

                        using (var reader = cmd.ExecuteReader(CommandBehavior.Default))
                        {
                            while (reader.Read())
                            {

                                var row = new Row();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = reader[i];
                                }

                                if(item[_leftColumn].Equals(row[_rightColumn]))
                                {
                                    item.Merge(row);
                                    Emit(item);
                                    emitted = true;
                                }
                            }
                        }
                    }
                    if (!emitted)
                    {
                        Emit(item);
                    }
                }
            }
            SignalEnd();
        }
    }
}
