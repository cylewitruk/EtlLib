using System;

namespace EtlLib.Nodes
{
    public interface IBlockingNode
    {
        INodeWaitSignaller WaitSignaller { get; }
        INode SetWaitSignaller(INodeWaitSignaller signaller);
    }

    public interface INodeWaitSignaller : IDisposable
    {
        void SignalWaitEnd();
    }

    public interface INodeWaiter
    {
        void Wait();
    }
}