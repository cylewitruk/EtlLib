using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessCompletedBuilderContext
    {
        void PrintGraph();
        EtlProcess Build();
    }

    public interface IEtlProcessCompletedWithResultBuilderContext<TOut> : IEtlProcessCompletedBuilderContext
        where TOut : class, IFreezable
    {
    }

    public class EtlProcessCompletedBuilderContext : IEtlProcessCompletedBuilderContext
    {
        private readonly EtlProcessBuilder _parentBuilder;
        private readonly ILogger _log;

        public EtlProcessCompletedBuilderContext(EtlProcessBuilder parentBuilder)
        {
            _parentBuilder = parentBuilder;
            _log = parentBuilder.Log;
        }

        public EtlProcess Build()
        {
            return _parentBuilder.Build();
        }

        public void PrintGraph()
        {
            var sb = new StringBuilder();

            sb.AppendLine();
            sb.AppendLine(new string('=', 80));
            sb.AppendLine($"ETL Process Builder Graph");
            sb.AppendLine($"* Id:   {_parentBuilder.Id}");
            sb.AppendLine($"* Name: {_parentBuilder.Name}");
            sb.AppendLine(new string('-', 80));
            sb.AppendLine($"[Root]: ETL Process '{_parentBuilder.Name}'");

            PrintTree(IOMapFromNode(_parentBuilder.FirstNode), "", true);

            // WRITE OUTPUT
            sb.AppendLine(new string('=', 80));
            _log.Info(sb.ToString());

            void PrintTree(InputOutputMap tree, string indent, bool last)
            {
                Type input, output;

                sb.Append($"{indent}+- {tree.ThisNode}");
                switch (tree.ThisNode)
                {
                    case INodeWithInput _ when tree.ThisNode is INodeWithOutput:
                        input = tree.ThisNode.GetType().GetInterface(typeof(INodeWithInput<>).FullName).GenericTypeArguments[0];
                        output = tree.ThisNode.GetType().GetInterface(typeof(INodeWithOutput<>).FullName).GenericTypeArguments[0];
                        sb.AppendLine($" [input({input.Name})/output({output.Name})]");
                        break;
                    case INodeWithInput _:
                        input = tree.ThisNode.GetType().GetInterface(typeof(INodeWithInput<>).FullName).GenericTypeArguments[0];
                        sb.AppendLine($" [input({input.Name})]");
                        break;
                    case INodeWithOutput _:
                        output = tree.ThisNode.GetType().GetInterface(typeof(INodeWithOutput<>).FullName).GenericTypeArguments[0];
                        sb.AppendLine($" [output({output.Name})]");
                        break;
                }

                indent += last ? "   " : "|  ";

                for (var i = 0; i < tree.TargetNodes.Count; i++)
                {
                    PrintTree(IOMapFromNode(tree.TargetNodes[i]), indent, i == tree.TargetNodes.Count - 1);
                }
            }

            InputOutputMap IOMapFromNode(INode node)
            {
                return _parentBuilder.NodeGraph[node.Id];
            }
            
        }
    }

    public class EtlProcessCompletedWithResultBuilderContext<TOut> : EtlProcessCompletedBuilderContext,
        IEtlProcessCompletedWithResultBuilderContext<TOut>
        where TOut : class, IFreezable
    {
        public EtlProcessCompletedWithResultBuilderContext(EtlProcessBuilder parentBuilder) 
            : base(parentBuilder)
        {
        }
    }
}