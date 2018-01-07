using System;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline.Builders;

namespace EtlLib
{
    public static class EtlProcessBuilderExtensions
    {
        public static IOutputNodeBuilderContext<T> Transform<T>(this IOutputNodeBuilderContext<T> builder, Func<T, T> transform)
            where T : class, IFreezable
        {
            return builder.Continue(ctx => new GenericTransformationNode<T>((state, row) => transform(row)));
        }

        public static IOutputNodeBuilderContext<T> Filter<T>(this IOutputNodeBuilderContext<T> builder, Func<T, bool> predicate)
            where T : class, IFreezable
        {
            return builder.Continue(ctx => new GenericFilterNode<T>(predicate));
        }

        public static IOutputNodeBuilderContext<TOut> Map<TIn, TOut>(this IOutputNodeBuilderContext<TIn> builder, Func<TIn, TOut> map)
            where TIn : class, IFreezable
            where TOut : class, IFreezable
        {
            return builder.Continue(ctx => new GenericMappingNode<TIn, TOut>(map));
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

                var newRow = row.Copy();
                newRow[idColumnName] = state[idColumnName];
                return newRow;
            }));
        }
    }
}