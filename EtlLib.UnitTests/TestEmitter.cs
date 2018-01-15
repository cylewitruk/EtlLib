using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;
using FluentAssertions;

namespace EtlLib.UnitTests
{
    public class TestEmitter<TOut> : IEmitter<TOut> 
        where TOut : class, INodeOutput<TOut>, new()
    {
        public IList<TOut> EmittedItems { get; }
        public bool HasSignalledEnd { get; private set; }

        public TestEmitter()
        {
            EmittedItems = new List<TOut>();
        }

        public void Emit(TOut item)
        {
            EmittedItems.Add(item);
        }

        public void SignalEnd()
        {
            HasSignalledEnd.Should().BeFalse("Node has already signalled the end of its enumeration.  Nodes should not call SignalEnd() more than once.");
            HasSignalledEnd = true;
        }
    }
}