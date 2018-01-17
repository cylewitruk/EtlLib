using System.Collections.Generic;
using System.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.PostgreSQL
{
    public class ExecutePostgresCommandNode : IEtlPipelineOperation
    {
        private readonly string _connectionString;
        private readonly string _commandText;
        private readonly Dictionary<string, object> _parameters;

        private IsolationLevel _isolationLevel;

        public string Name { get; }

        public ExecutePostgresCommandNode(string name, string connectionString, string commandText)
        {
            Name = name;
            _connectionString = connectionString;
            _commandText = commandText;
            _parameters = new Dictionary<string, object>();
        }

        public ExecutePostgresCommandNode WithParameter(string name, object value)
        {
            _parameters[name] = value;
            return this;
        }

        public ExecutePostgresCommandNode WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public IEtlPipelineOperationResult Execute()
        {
            using (var con = new Npgsql.NpgsqlConnection(_connectionString))
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
                        p.Value = param.Value;

                        cmd.Parameters.Add(p);
                    }

                    cmd.ExecuteNonQuery();
                }
            }

            return new EtlPipelineOperationResult(true);
        }
    }
}