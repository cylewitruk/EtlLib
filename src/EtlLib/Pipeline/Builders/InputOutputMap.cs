using System.Collections.Generic;
using System.Linq;
using EtlLib.Nodes;

namespace EtlLib.Pipeline.Builders
{
    public class InputOutputMap
    {
        public INode ThisNode { get; set; }
        public List<ISinkNode> TargetNodes { get; }
        public List<ISourceNode> SourceNodes { get; }
        public EtlProcessBuilder Builder { get; }

        public InputOutputMap(INode thisNode, EtlProcessBuilder builder)
        {
            TargetNodes = new List<ISinkNode>();
            SourceNodes = new List<ISourceNode>();
            ThisNode = thisNode;
            Builder = builder;
        }

        public InputOutputMap AddTargetNode(ISinkNode target)
        {
            TargetNodes.Add(target);
            return this;
        }

        public InputOutputMap AddSourceNode(ISourceNode source)
        {
            SourceNodes.Add(source);
            return this;
        }

        public override string ToString()
        {
            var sources = string.Join(", ", SourceNodes.Select(x => x.GetType().Name));
            var targets = string.Join(", ", TargetNodes.Select(x => x.GetType().Name));
            return $"{ThisNode.GetType().Name}, Builder='{Builder.Name}', Sources=[{sources}], Targets=[{targets}]";
        }
    }
}