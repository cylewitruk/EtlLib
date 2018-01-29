using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib
{
    public static class EtlPipelineExtensions
    {
        public static IEtlPipeline EnsureDirectoryTreeExists(this IEtlPipeline pipeline, Func<EtlPipelineContext, string> path)
        {
            var method = new Action(() =>
            {
                var value = path(pipeline.Context);
                new EnsureDirectoryExistsEtlOperation(value).Execute(pipeline.Context);
            });

            return pipeline.Run(new DynamicInvokeEtlOperation(method).Named("Ensure Directory Exists (Dynamic)"));
        }

        public static IEtlPipeline EnsureDirectoryTreeExists(this IEtlPipeline pipeline, string path)
        {
            return pipeline.Run(new EnsureDirectoryExistsEtlOperation(path).Named($"Ensure Directory Exists ({path})"));
        }

        public static IEtlPipeline AppendResult<T>(this IEtlPipelineEnumerableResultContext<T> ctx,
            ICollection<T> collection)
        {
            var method = new Action(() =>
            {
                var result = (IEnumerableEtlOperationResult<T>)ctx.Pipeline.LastResult;
                foreach (var item in result.Result)
                    collection.Add(item);
            });

            return ctx.Pipeline.Run(new DynamicInvokeEtlOperation(method).Named("Append Result"));
        }
    }
}