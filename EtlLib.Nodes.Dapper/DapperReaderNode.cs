using System;
using EtlLib.Data;

namespace EtlLib.Nodes.Dapper
{
    public class DapperReaderNode<TOut> : AbstractOutputNode<TOut> 
        where TOut : class, INodeOutput<TOut>, new()
    {
        public override void OnExecute()
        {
            throw new NotImplementedException();
        }
    }
}
