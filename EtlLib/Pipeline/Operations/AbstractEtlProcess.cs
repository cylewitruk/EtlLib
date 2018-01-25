using System;
using EtlLib.Data;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlProcess : AbstractEtlOperationWithNoResult
    {
        private IEtlOperationWithNoResult _etlProcess;

        protected void Build(Action<IEtlProcessBuilder> builder)
        {
            var b = EtlProcessBuilder.Create();
            builder(b);

            _etlProcess = ((EtlProcessBuilder)b).Build();
            Named(_etlProcess.Name);
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return _etlProcess.Execute(context);
        }
    }

    public abstract class AbstractEtlProcess<T> : AbstractEtlOperationWithEnumerableResult<T>
        where T : class, INodeOutput<T>, new()
    {
        private IEtlOperationWithEnumerableResult<T> _etlProcess;

        protected void Build(Action<IEtlProcessBuilder> builder)
        {
            var b = EtlProcessBuilder.Create();
            builder(b);

            _etlProcess = ((EtlProcessBuilder)b).Build<T>();
            Named(_etlProcess.Name);
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return _etlProcess.Execute(context);
        }
    }
}