using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Builders;

namespace EtlLib
{
    public static class EtlProcessBuilderExtensions
    {
        public static IOutputNodeBuilderContext<T> Transform<T>(this IOutputNodeBuilderContext<T> builder, Func<EtlProcessContext, T, T> transform)
            where T : class, INodeOutput<T>, new()
        {
            return builder.Continue(ctx => new GenericTransformationNode<T>((state, row) => transform(ctx, row)));
        }

        public static IOutputNodeBuilderContext<T> Filter<T>(this IOutputNodeBuilderContext<T> builder, Func<T, bool> predicate)
            where T : class, INodeOutput<T>, new()
        {
            return builder.Continue(ctx => new GenericFilterNode<T>(predicate));
        }

        public static IOutputNodeBuilderContext<TOut> Map<TIn, TOut>(this IOutputNodeBuilderContext<TIn> builder, Func<TIn, TOut, TOut> map)
            where TIn : class, INodeOutput<TIn>, new()
            where TOut : class, INodeOutput<TOut>, new()
        {
            return builder.Continue(ctx => new GenericMappingNode<TIn, TOut>(map));
        }

        public static IOutputNodeBuilderContext<TOut> GenerateInput<TOut, TState>(
            this IEtlProcessBuilder builder,
            Func<GenericDataGenerationNode<TOut, TState>.IDataGeneratorHelper<TState>, bool> @while,
            Func<int, GenericDataGenerationNode<TOut,TState>.IDataGeneratorHelper<TState>, TOut> generateFn)
            where TOut : class, INodeOutput<TOut>, new()
        {
            return builder.Input(ctx => new GenericDataGenerationNode<TOut, TState>(@while, generateFn));
        }

        public static IOutputNodeBuilderContext<Row> Classify(this IOutputNodeBuilderContext<Row> builder,
            string outputColumn, Action<GenericClassificationNode<Row, string, object>> cat)
        {
            var node = new GenericClassificationNode<Row, string, object>(row => row[outputColumn]);
            cat(node);
            return builder.Continue(ctx => node);
        }

        public static IOutputNodeBuilderContext<Row> GenerateRowNumbers(this IOutputNodeBuilderContext<Row> builder,
            string idColumnName)
        {
            return builder.Continue(ctx => new GenericTransformationNode<Row>((state, row) =>
            {
                if (!state.ContainsKey(idColumnName))
                    state[idColumnName] = 0;
                else
                    state[idColumnName] = (int) state[idColumnName] + 1;

                //var newRow = row.Copy();
                //newRow[idColumnName] = state[idColumnName];
                //return newRow;
                var newRow = ctx.ObjectPool.Borrow<Row>();
                row.CopyTo(newRow);
                newRow[idColumnName] = state[idColumnName];
                ctx.ObjectPool.Return(row);
                return newRow;
            }));
        }
    }
}