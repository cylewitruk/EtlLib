using System.Collections.Generic;
using EtlLib.Data;

namespace EtlLib.Nodes
{
    public abstract class AbstractInputOutputNode<TIn, TOut> : AbstractOutputNode<TOut>, INodeWithInputOutput<TIn, TOut>
        where TIn : class, INodeOutput<TIn>, new()
        where TOut : class, INodeOutput<TOut>, new()
    {
        public IEnumerable<TIn> Input { get; private set; }

        public INodeWithInput<TIn> SetInput(IEnumerable<TIn> input)
        {
            Input = input;
            return this;
        }
    }
}