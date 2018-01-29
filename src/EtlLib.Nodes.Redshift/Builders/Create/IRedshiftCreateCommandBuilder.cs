using System;

namespace EtlLib.Nodes.Redshift.Builders.Create
{
    public interface IRedshiftCreateCommandBuilder
    {
        void Table(string tableName, Action<IRedshiftCreateTableCommandBuilder> tbl);
        //IRedshiftCreateCommandBuilder TableAs(string tableName, )
    }

    public class RedshiftCreateCommandBuilder : IRedshiftBuilder, IRedshiftCreateCommandBuilder
    {
        private IRedshiftBuilder _builder;

        public void Table(string tableName, Action<IRedshiftCreateTableCommandBuilder> tbl)
        {
            var builder = new RedshiftCreateTableCommandBuilder(tableName);
            tbl(builder);
            _builder = builder;
        }

        public string Build()
        {
            return _builder.Build();
        }
    }
}