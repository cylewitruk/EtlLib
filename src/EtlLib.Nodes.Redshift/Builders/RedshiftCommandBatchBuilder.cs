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
    }
}