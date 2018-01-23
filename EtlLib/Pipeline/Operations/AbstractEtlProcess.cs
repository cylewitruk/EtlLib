using System;
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
}