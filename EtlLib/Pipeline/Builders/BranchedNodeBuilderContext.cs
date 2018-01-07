using System;
using System.Linq;
using EtlLib.Data;
using EtlLib.Nodes;
using EtlLib.Nodes.Impl;

namespace EtlLib.Pipeline.Builders
{
    public interface IBranchedNodeBuilderContext<TOut>
        where TOut : class, IFreezable
    {
        IOutputNodeBuilderContext<TOut> MergeResults();
    }

    public class BranchedNodeBuilderContext<TOut> : IBranchedNodeBuilderContext<TOut> 
        where TOut : class, IFreezable
    {
        private readonly EtlProcessBuilder _parentBuilder, _branch1Builder, _branch2Builder;
        
        public INode CreatingNode { get; }

        public BranchedNodeBuilderContext(EtlProcessBuilder parentBuilder, EtlProcessBuilder branch1Builder, EtlProcessBuilder branch2Builder, INode creatingNode)
        {
            _parentBuilder = parentBuilder;
            _branch1Builder = branch1Builder;
            _branch2Builder = branch2Builder;

            CreatingNode = creatingNode;
        }

        public IOutputNodeBuilderContext<TOut> MergeResults()
        {
            foreach (var item in _branch1Builder.NodeGraph.Where(x => x.Key != CreatingNode.Id))
                _parentBuilder.NodeGraph.Add(item.Key, item.Value);

            foreach (var item in _branch2Builder.NodeGraph.Where(x => x.Key != CreatingNode.Id))
                _parentBuilder.NodeGraph.Add(item.Key, item.Value);

            _parentBuilder.RegisterNode(new GenericMergeNode<TOut>(), (current, last) =>
            {
                // Set the source nodes of the current map to the last nodes of the subprocess builders
                current
                    .AddSourceNode((INodeWithOutput) _branch1Builder.LastNode)
                    .AddSourceNode((INodeWithOutput) _branch2Builder.LastNode);

                // Add merge node as target for last node of subprocess builder 1 
                _parentBuilder.NodeGraph[_branch1Builder.LastNode.Id]
                    .AddTargetNode((INodeWithInput) current.ThisNode);

                // Add merge node as target for last node of subprocess builder 2
                _parentBuilder.NodeGraph[_branch2Builder.LastNode.Id]
                    .AddTargetNode((INodeWithInput)current.ThisNode);

                _parentBuilder.Log.Debug($"'{_parentBuilder.Name}' registered new merge for branching operation '{_branch1Builder.Name}' and '{_branch2Builder.Name}'");
                _parentBuilder.Log.Debug($"'{_branch1Builder.Name}' registered [output from] {_branch1Builder.LastNode} as [input to] target -> {current.ThisNode}");
                _parentBuilder.Log.Debug($"'{_branch1Builder.Name}' registered [output from] {_branch2Builder.LastNode} as [input to] target -> {current.ThisNode}");
            });

            return new OutputNodeBuilderContext<TOut>(_parentBuilder, CreatingNode);
        }
    }
}