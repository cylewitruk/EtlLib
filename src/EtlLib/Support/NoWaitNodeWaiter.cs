using EtlLib.Nodes;

namespace EtlLib.Support
{
    public class NoWaitNodeWaiter : INodeWaiter
    {
        public static NoWaitNodeWaiter Instance;

        static NoWaitNodeWaiter()
        {
            Instance = new NoWaitNodeWaiter();
        }

        public void Wait()
        {
        }
    }
}