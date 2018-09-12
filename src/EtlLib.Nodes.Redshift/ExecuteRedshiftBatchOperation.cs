using System;
using System.Collections.Generic;
using System.Data;
using EtlLib.Nodes.Redshift.Builders;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;
    
namespace EtlLib.Nodes.Redshift
{
    public class ExecuteRedshiftBatchOperation : AbstractEtlOperationWithNoResult
    {
        private readonly string _connectionName;
        private readonly List<string> _commands;
 
        public ExecuteRedshiftBatchOperation(string name, string connectionName, Action<RedshiftCommandBatchBuilder> red)
        {
            Named(name);
            _connectionName = connectionName;

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

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            var log = context.GetLogger(GetType().FullName);

            log.Debug("Opening connection to Redshift.");
            
            using (var con = context.CreateNamedDbConnection(_connectionName)) //new NpgsqlConnection(_connectionString))
            {
                con.Open();
                foreach (var redshiftCommand in _commands)
                {
                    log.Debug($"Executing Redshift command: {redshiftCommand}");
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
                        log.Error(e.Message);
                        return new EtlOperationResult(false)
                            .WithError(this, e, redshiftCommand);
                    }
                }
            }

            return new EtlOperationResult(true);
        }
    }
}