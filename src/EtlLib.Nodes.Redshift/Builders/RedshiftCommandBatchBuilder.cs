using System;
using System.Collections.Generic;

namespace EtlLib.Nodes.Redshift.Builders
{
    public class RedshiftCommandBatchBuilder
    {
        public List<string> Commands { get; }

        public RedshiftCommandBatchBuilder()
        {
            Commands = new List<string>();
        }

        public void Execute(Action<IRedshiftCommandBuilder> cmd)
        {
            var builder = new RedshiftCommandBuilder();
            cmd(builder);

            Commands.Add(builder.Build());
        }

        public void Execute(IRedshiftBuilder cmd)
        {
            Commands.Add(cmd.Build());
        }

        public void ExecuteIf(Func<bool> predicate, Action<IRedshiftCommandBuilder> cmd)
        {
            if (!predicate())
                return;

            Execute(cmd);
        }

        public void ExecuteIf(Func<bool> predicate, IRedshiftBuilder cmd)
        {
            if (!predicate())
                return;

            Execute(cmd);
        }

        public void BeginTransaction()
        {
            Commands.Add("BEGIN;");
        }

        public void CommitTransaction()
        {
            Commands.Add("COMMIT;");
        }

        public void RollbackTransaction()
        {
            Commands.Add("ROLLBACK;");
        }

        public void Truncate(string tableName)
        {
            Commands.Add($"TRUNCATE TABLE {tableName}");
        }
    }
}