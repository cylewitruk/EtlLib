using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericEnumerableReaderNode<T> : AbstractSourceNode<T> where T : class, INodeOutput<T>, new()
    {
        private readonly IEnumerable<T> _enumerable;

        public GenericEnumerableReaderNode(IEnumerable<T> enumerable)
        {
            _enumerable = enumerable;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach(var item in _enumerable)
                Emit(item);

            SignalEnd();
        }
    }
}