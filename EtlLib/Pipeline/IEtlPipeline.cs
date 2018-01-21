using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline
    {
        EtlPipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, IEtlProcessBuilder> builder);
        IEtlPipeline Run(IEtlOperation operation);
        IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx);
        IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithEnumerableResult<TOut> operation);
        IEtlPipelineWithScalarResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithScalarResult<TOut> operation);
        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlOperation>> ctx);
    }
}