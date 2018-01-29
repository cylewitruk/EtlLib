using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        EtlPipelineContext Context { get; }
        IEtlOperationResult LastResult { get; }

        EtlPipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, IEtlProcessBuilder> builder);
        IEtlPipeline Run(IEtlOperation operation);
        IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx);

        IEtlPipeline Run<TOut>(IEtlOperationWithEnumerableResult<TOut> operation, 
            Action<IEtlPipelineEnumerableResultContext<TOut>> result);
        IEtlPipeline Run<TOut>(Func<EtlPipelineContext, IEtlOperationWithEnumerableResult<TOut>> operation,
            Action<IEtlPipelineEnumerableResultContext<TOut>> result);

        IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithEnumerableResult<TOut> operation);
        IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(Func<EtlPipelineContext, 
            IEtlOperationWithEnumerableResult<TOut>> operation);
        IEtlPipelineWithScalarResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithScalarResult<TOut> operation);

        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlOperation>> ctx);
    }
}