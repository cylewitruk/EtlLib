using System.Threading;
using EtlLib.Nodes;

namespace EtlLib.Support
{
    public class BlockingWaitSignaller : INodeWaitSignaller, INodeWaiter
    {
        private readonly ManualResetEventSlim _resetEvent;

        public BlockingWaitSignaller()
        {
            _resetEvent = new ManualResetEventSlim();
        }

        public void SignalWaitEnd()
        {
            _resetEvent.Set();
        }

        public void Wait()
        {
            _resetEvent.Wait();
        }

        public void Dispose()
        {
            _resetEvent.Dispose();
        }
    }
}