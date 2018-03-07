using System;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipelineWithScalarResultContext<out TOut>
    {
        IEtlPipeline Pipeline { get; }

        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, TOut> result);
    }

    public class EtlPipelineWithScalarResultContext<TOut> : IEtlPipelineWithScalarResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

        public IEtlPipeline Pipeline => _parentPipeline;

        public EtlPipelineWithScalarResultContext(EtlPipeline pipeline, EtlPipelineContext context)
        {
            _parentPipeline = pipeline;
            _context = context;
        }

        public IEtlPipeline SaveResult(string stateKeyName)
        {
            var method = new Action(() =>
            {
                var result = (IScalarEtlOperationResult<TOut>)_parentPipeline.LastResult;
                _context.State[stateKeyName] = result.Result;
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("Save Scalar Result"));
        }

        public IEtlPipeline WithResult(Action<EtlPipelineContext, TOut> result)
        {
            var method = new Action(() =>
            {
                var value = ((IScalarEtlOperationResult<TOut>) _parentPipeline.LastResult).Result;
                result(_context, value);
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("With Scalar Result"));
        }
    }
}