using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipelineEnumerableResultContext<out TOut>
    {
        IEtlPipeline Pipeline { get; }

        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, IEnumerable<TOut>> result);
        IEtlPipeline ForEachResult(Action<EtlPipelineContext, int, TOut> result);
    }

    public class EtlPipelineEnumerableResultContext<TOut> : IEtlPipelineEnumerableResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

        public IEtlPipeline Pipeline => _parentPipeline;

        public EtlPipelineEnumerableResultContext(EtlPipeline pipeline, EtlPipelineContext context)
        {
            _parentPipeline = pipeline;
            _context = context;
        }

        public IEtlPipeline SaveResult(string stateKeyName)
        {
            var method = new Action(() =>
            {
                var result = (IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult;
                _context.State[stateKeyName] = result.Result;
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("Save Enumerable Result"));
        }

        public IEtlPipeline WithResult(Action<EtlPipelineContext, IEnumerable<TOut>> result)
        {
            var method = new Action(() =>
            {
                var value = ((IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult).Result;
                result(_context, value);
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("With Enumerable Result"));
        }

        public IEtlPipeline ForEachResult(Action<EtlPipelineContext, int, TOut> result)
        {
            var method = new Action(() =>
            {
                var results = ((IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult).Result;

                var count = 0;
                foreach (var item in results)
                {
                    result(_context, ++count, item);
                }
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named($"Foreach {typeof(TOut).Name} in Result"));
        }
    }
}