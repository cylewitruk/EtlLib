using System;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlProcess : AbstractEtlPipelineOperation
    {
        private EtlProcess _etlProcess;

        protected void Build(Action<IEtlProcessBuilder> builder)
        {
            var b = EtlProcessBuilder.Create();
            builder(b);

            _etlProcess = ((EtlProcessBuilder)b).Build();
            SetName(_etlProcess.Name);
        }

        public override IEtlPipelineOperationResult Execute()
        {
            return _etlProcess.Execute();
        }

        public override void OnContextChanged(EtlPipelineContext newContext)
        {
            _etlProcess.SetContext(newContext);
        }
    }
}