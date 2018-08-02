using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericResultCollectionNode<TIn> : AbstractSinkNode<TIn>, IResultCollectorNode
        where TIn : class, INodeOutput<TIn>, new()
    {
        private readonly List<TIn> _results;

        public int Count { get; private set; }
        public IEnumerable<TIn> Result => _results;

        public GenericResultCollectionNode()
        {
            _results = new List<TIn>();
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                _results.Add(item);
                Count++;
            }
        }
    }
}