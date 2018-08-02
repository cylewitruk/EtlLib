using System.Collections.Generic;
using EtlLib.Data;

namespace EtlLib.Nodes
{
    public abstract class AbstractSinkNode<TIn> : AbstractNode, ISinkNode<TIn>
        where TIn : class, INodeOutput<TIn>, new()
    {
        public IEnumerable<TIn> Input { get; private set; }
        public ISinkNode<TIn> SetInput(IEnumerable<TIn> input)
        {
            Input = input;
            return this;
        }
    }
}