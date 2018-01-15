using System;
using System.Collections.Generic;
using EtlLib.Nodes.Redshift.Builders;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Redshift
{
    public class ExecuteRedshiftCommandNode : IExecutableNode
    {
        public string Name { get; }

        private readonly string _connectionString;
        private readonly List<string> _commands;
 
        public ExecuteRedshiftCommandNode(string name, string connectionString, Action<RedshiftCommandBatchBuilder> red)
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
            throw new NotImplementedException();
        }
    }
}