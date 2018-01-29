using EtlLib.Nodes.Redshift.Builders.Copy;
using EtlLib.Nodes.Redshift.Builders.Create;

namespace EtlLib.Nodes.Redshift.Builders
{
    public interface IRedshiftCommandBuilder
    {
        IRedshiftCreateCommandBuilder Create { get; }
        IRedshiftCopyCommandBuilder Copy { get; }
    }

    public class RedshiftCommandBuilder : IRedshiftCommandBuilder, IRedshiftBuilder
    {
        private IRedshiftBuilder _builder;

        public IRedshiftCreateCommandBuilder Create
        {
            get
            {
                var builder = new RedshiftCreateCommandBuilder();
                _builder = builder;
                return builder;
            }
        }

        public IRedshiftCopyCommandBuilder Copy
        {
            get
            {
               var builder = new RedshiftCopyCommandBuilder();
                _builder = builder;
                return builder;
            }
        }

        public string Build()
        {
            return _builder.Build();
        }
    }
}