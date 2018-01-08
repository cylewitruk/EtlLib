using System.Collections.Generic;
using EtlLib.Data;

namespace EtlLib.Nodes
{
    public abstract class AbstractInputNode<TIn> : Node, INodeWithInput<TIn>
        where TIn : class, INodeOutput<TIn>, new()
    {
        public IEnumerable<TIn> Input { get; private set; }
        public INodeWithInput<TIn> SetInput(IEnumerable<TIn> input)
        {
            Input = input;
            return this;
        }
    }
}