using System;
using System.Collections.Generic;
using System.Text;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessCompletedBuilderContext
    {
        void PrintGraph();
        EtlProcess Build();
    }

    public interface IEtlProcessCompletedWithResultBuilderContext<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        IEnumerable<TOut> Result { get; }

        void PrintGraph();
        EtlProcess<TOut> Build();
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

            sb.AppendLine("\n");
            sb.AppendLine(new string('=', 80));
            sb.AppendLine($"ETL Process Builder Graph");
            sb.AppendLine($"* Id:   {_parentBuilder.Id}");
            sb.AppendLine($"* Name: {_parentBuilder.Name}");
            sb.AppendLine(new string('-', 80));
            sb.AppendLine($"[Root]: ETL Process '{_parentBuilder.Name}'");

            PrintTree(IoMapFromNode(_parentBuilder.FirstNode), "", true);

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
                    PrintTree(IoMapFromNode(tree.TargetNodes[i]), indent, i == tree.TargetNodes.Count - 1);
                }
            }

            InputOutputMap IoMapFromNode(INode node)
            {
                return _parentBuilder.NodeGraph[node.Id];
            }
            
        }
    }

    public class EtlProcessCompletedWithResultBuilderContext<TOut> : IEtlProcessCompletedWithResultBuilderContext<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly EtlProcessBuilder _parentBuilder;
        private readonly ILogger _log;

        public IEnumerable<TOut> Result { get; }

        public EtlProcessCompletedWithResultBuilderContext(EtlProcessBuilder parentBuilder, IEnumerable<TOut> result)
        {
            _parentBuilder = parentBuilder;
            _log = parentBuilder.Log;
            Result = result;
        }

        public void PrintGraph()
        {
            var sb = new StringBuilder();

            sb.AppendLine("\n");
            sb.AppendLine(new string('=', 80));
            sb.AppendLine($"ETL Process Builder Graph");
            sb.AppendLine($"* Id:   {_parentBuilder.Id}");
            sb.AppendLine($"* Name: {_parentBuilder.Name}");
            sb.AppendLine(new string('-', 80));
            sb.AppendLine($"[Root]: ETL Process '{_parentBuilder.Name}'");

            PrintTree(IoMapFromNode(_parentBuilder.FirstNode), "", true);

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
                        input = tree.ThisNode.GetType().GetInterface(typeof(INodeWithInput<>).FullName)
                            .GenericTypeArguments[0];
                        output = tree.ThisNode.GetType().GetInterface(typeof(INodeWithOutput<>).FullName)
                            .GenericTypeArguments[0];
                        sb.AppendLine($" [input({input.Name})/output({output.Name})]");
                        break;
                    case INodeWithInput _:
                        input = tree.ThisNode.GetType().GetInterface(typeof(INodeWithInput<>).FullName)
                            .GenericTypeArguments[0];
                        sb.AppendLine($" [input({input.Name})]");
                        break;
                    case INodeWithOutput _:
                        output = tree.ThisNode.GetType().GetInterface(typeof(INodeWithOutput<>).FullName)
                            .GenericTypeArguments[0];
                        sb.AppendLine($" [output({output.Name})]");
                        break;
                }

                indent += last ? "   " : "|  ";

                for (var i = 0; i < tree.TargetNodes.Count; i++)
                {
                    PrintTree(IoMapFromNode(tree.TargetNodes[i]), indent, i == tree.TargetNodes.Count - 1);
                }
            }

            InputOutputMap IoMapFromNode(INode node)
            {
                return _parentBuilder.NodeGraph[node.Id];

            }
        }

        public EtlProcess<TOut> Build()
        {
            return _parentBuilder.Build<TOut>();
        }
    }
}