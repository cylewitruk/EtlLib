using System;
using System.Collections.Generic;
using System.Data;
using EtlLib.Nodes.Redshift.Builders;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;
using Npgsql;

namespace EtlLib.Nodes.Redshift
{
    public class ExecuteRedshiftBatchOperation : AbstractEtlOperationWithNoResult
    {
        private readonly string _connectionString;
        private readonly List<string> _commands;
 
        public ExecuteRedshiftBatchOperation(string name, string connectionString, Action<RedshiftCommandBatchBuilder> red)
        {
            Named(name);
            _connectionString = connectionString;

            var builder = new RedshiftCommandBatchBuilder();
            red(builder);
            
            _commands = new List<string>();

            foreach (var t in builder.Commands)
            {
                var cmd = t;
                if (!cmd.EndsWith(";"))
                    cmd = cmd.TrimEnd('\n', '\r') + ";";
                _commands.Add(cmd);
            }
        }

        public override IEtlOperationResult Execute()
        {
            using (var con = new NpgsqlConnection(_connectionString))
            {
                con.Open();
                foreach (var redshiftCommand in _commands)
                {
                    try
                    {
                        using (var cmd = con.CreateCommand())
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = redshiftCommand;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception e)
                    {
                        return new EtlOperationResult(false)
                            .WithError(this, e, redshiftCommand);
                    }
                }
            }

            return new EtlOperationResult(true);
        }
    }
}