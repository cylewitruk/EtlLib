using System.Collections.Generic;
using EtlLib.Data;

namespace EtlLib.Nodes
{
    public abstract class AbstractInputOutputNode<TIn, TOut> : AbstractOutputNode<TOut>, INodeWithInputOutput<TIn, TOut>
        where TIn : class, IFreezable
        where TOut : class, IFreezable
    {
        public IEnumerable<TIn> Input { get; private set; }

        public INodeWithInput<TIn> SetInput(IEnumerable<TIn> input)
        {
            Input = input;
            return this;
        }
    }
}