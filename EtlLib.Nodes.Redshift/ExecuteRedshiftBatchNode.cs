using System;
using System.Collections.Generic;
using System.Data;
using EtlLib.Nodes.Redshift.Builders;
using EtlLib.Pipeline;
using Npgsql;

namespace EtlLib.Nodes.Redshift
{
    public class ExecuteRedshiftBatchNode : IExecutableNode
    {
        public string Name { get; }

        private readonly string _connectionString;
        private readonly List<string> _commands;
 
        public ExecuteRedshiftBatchNode(string name, string connectionString, Action<RedshiftCommandBatchBuilder> red)
        {
            Name = name;
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

        public void Execute()
        {
            using (var con = new NpgsqlConnection(_connectionString))
            {
                con.Open();
                foreach (var redshiftCommand in _commands)
                {
                    using (var cmd = con.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = redshiftCommand;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}