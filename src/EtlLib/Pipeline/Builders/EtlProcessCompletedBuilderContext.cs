using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline.Builders
{
    public interface IEtlProcessCompletedBuilderContext
    {
        IEtlOperationWithNoResult Build();
    }

    public interface IEtlProcessCompletedWithResultBuilderContext<out TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        IEnumerable<TOut> Result { get; }
        
        IEtlOperationWithEnumerableResult<TOut> Build();
    }

    public class EtlProcessCompletedBuilderContext : IEtlProcessCompletedBuilderContext
    {
        private readonly EtlProcessBuilder _parentBuilder;

        public EtlProcessCompletedBuilderContext(EtlProcessBuilder parentBuilder)
        {
            _parentBuilder = parentBuilder;
        }

        public IEtlOperationWithNoResult Build()
        {
            return _parentBuilder.Build();
        }
    }

    public class EtlProcessCompletedWithResultBuilderContext<TOut> : IEtlProcessCompletedWithResultBuilderContext<TOut>
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly EtlProcessBuilder _parentBuilder;

        public IEnumerable<TOut> Result { get; }

        public EtlProcessCompletedWithResultBuilderContext(EtlProcessBuilder parentBuilder, IEnumerable<TOut> result)
        {
            _parentBuilder = parentBuilder;
            Result = result;
        }

        public IEtlOperationWithEnumerableResult<TOut> Build()
        {
            return _parentBuilder.Build<TOut>();
        }
    }
}