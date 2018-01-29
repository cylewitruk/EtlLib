namespace EtlLib.Nodes
{
    public abstract class AbstractBlockingNode : AbstractNode, IBlockingNode
    {
        public INodeWaitSignaller WaitSignaller { get; private set; }
        public INode SetWaitSignaller(INodeWaitSignaller signaller)
        {
            WaitSignaller = signaller;
            return this;
        }
    }
}