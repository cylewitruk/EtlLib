using System;

namespace EtlLib.Nodes.Redshift.Builders
{
    public class RedshiftCommandBuilderException : Exception
    {
        public RedshiftCommandBuilderException(string message) 
            : base(message)
        {
        }
    }
}