using System.Collections.Generic;
using System.Threading.Tasks;
using EtlLib.Data;

namespace EtlLib.Nodes.Impl
{
    public class GenericMergeNode<T> : AbstractOutputNode<T>, INodeWithInput2<T>
        where T : class, INodeOutput<T>, new()
    {
        public IEnumerable<T> Input { get; private set; }
        public IEnumerable<T> Input2 { get; private set; }

        public INodeWithInput<T> SetInput(IEnumerable<T> input)
        {
            Input = input;
            return this;
        }
        
        public INodeWithInput2<T> SetInput2(IEnumerable<T> input2)
        {
            Input2 = input2;
            return this;
        }

        public override void Execute()
        {
            using (var input1Enumerator = Input.GetEnumerator())
            using (var input2Enumerator = Input2.GetEnumerator())
            {
                var input1HasItems = input1Enumerator.MoveNext();
                var input2HasItems = input2Enumerator.MoveNext();

                while (input1HasItems && input2HasItems)
                {
                    Emit(input1Enumerator.Current);
                    input1HasItems = input1Enumerator.MoveNext();

                    Emit(input2Enumerator.Current);
                    input2HasItems = input2Enumerator.MoveNext();
                }

                while (input1HasItems)
                {
                    Emit(input1Enumerator.Current);
                    input1HasItems = input1Enumerator.MoveNext();
                }

                while (input2HasItems)
                {
                    Emit(input2Enumerator.Current);
                    input2HasItems = input2Enumerator.MoveNext();
                }

                Emitter.SignalEnd();
            }
        }
    }
}