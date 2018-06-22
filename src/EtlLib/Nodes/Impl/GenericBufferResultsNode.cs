using System.Collections.Generic;
using System.Linq;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericBufferResultsNode<TIn> : AbstractProcessingNode<TIn, BufferedNodeOutput<TIn>> 
        where TIn : class, INodeOutput<TIn>, new() 
    {
        public override void OnExecute(EtlPipelineContext context)
        {
            var count = 0;
            var buffer = new List<TIn>();

            foreach (var item in Input)
            {
                count++;
                buffer.Add(item);
            }
            Emit(new BufferedNodeOutput<TIn>(buffer, count));
            SignalEnd();
        }
    }

    public class BufferedNodeOutput<TIn> : Frozen<BufferedNodeOutput<TIn>>, INodeOutput<BufferedNodeOutput<TIn>>
        where TIn : class, INodeOutput<TIn>, new()
    {
        public IEnumerable<TIn> BufferedOutput { get; private set; }
        public int Count { get; }

        public BufferedNodeOutput()
        {
            BufferedOutput = new List<TIn>();
        }

        public BufferedNodeOutput(IEnumerable<TIn> bufferedOutput, int count)
        {
            BufferedOutput = bufferedOutput;
            Count = count;
        }

        public void CopyTo(BufferedNodeOutput<TIn> obj)
        {
            BufferedOutput = new List<TIn>(obj.BufferedOutput);
        }

        public void Reset()
        {
            BufferedOutput = null;
        }
    }
}