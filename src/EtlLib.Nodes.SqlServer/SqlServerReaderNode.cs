using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.SqlServer
{
    public class SqlServerReaderNode : AbstractSourceNode<Row>
    {
        private readonly string _connectionString;
        private readonly string _commandText;
        private Dictionary<string, Func<object>> _parameters;

        private IsolationLevel _isolationLevel;

        public SqlServerReaderNode(string connectionString, string commandText)
        {
            _connectionString = connectionString;
            _commandText = commandText;
            _parameters = new Dictionary<string, Func<object>>();
            _isolationLevel = IsolationLevel.ReadCommitted;
        }

        public SqlServerReaderNode WithParameter(string name, Func<object> func)
        {
            _parameters[name] = func;
            return this;
        }

        public SqlServerReaderNode WithParameters(Dictionary<string, Func<object>> parameters)
        {
            _parameters = parameters;
            return this;
        }

        public SqlServerReaderNode WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var logger = context.GetLogger(GetType().FullName);

            try
            {
                using (var con = new SqlConnection(_connectionString))
                {
                    con.Open();

                    using (var trx = con.BeginTransaction(_isolationLevel))
                    using (var cmd = con.CreateCommand())
                    {
                        cmd.CommandText = _commandText;
                        cmd.CommandType = CommandType.Text;
                        cmd.Transaction = trx;

                        foreach (var param in _parameters)
                        {
                            var p = cmd.CreateParameter();
                            p.ParameterName = param.Key;
                            p.Value = param.Value();

                            cmd.Parameters.Add(p);
                        }

                        using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                        {
                            if (!reader.HasRows)
                            {
                                SignalEnd();
                                return;
                            }

                            while (reader.Read())
                            {
                                var row = new Row();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = reader[i] is DBNull ? null : reader[i];
                                }

                                Emit(row);
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                logger.Error($"Error while executing query: \"{_commandText}\"", e);
                throw;
            }
            finally
            {
                SignalEnd();
            }
        }
    }
}
