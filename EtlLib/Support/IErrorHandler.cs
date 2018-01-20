using System;
using EtlLib.Data;
using EtlLib.Nodes;

namespace EtlLib.Support
{
    public interface IErrorHandler
    {
        void RaiseError(INode node, Exception e);
        void RaiseError(INode node, Exception e, INodeOutput item);
    }
}