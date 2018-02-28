using System;
using EtlLib.Data;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline.Operations
{
    public abstract class AbstractEtlProcess : AbstractEtlOperationWithNoResult
    {
        private IEtlOperationWithNoResult _etlProcess;
        private IEtlProcessBuilder _builder;
        private Action<IEtlProcessBuilder> _bootstrapBuilder;

        protected void Build(Action<IEtlProcessBuilder> builder)
        {
            _bootstrapBuilder = builder;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            _builder = EtlProcessBuilder.Create(context);
            _bootstrapBuilder(_builder);

            _etlProcess = ((EtlProcessBuilder)_builder).Build();
            Named(_etlProcess.Name);

            return _etlProcess.Execute(context);
        }
    }

    public abstract class AbstractEtlProcess<T> : AbstractEtlOperationWithEnumerableResult<T>
        where T : class, INodeOutput<T>, new()
    {
        private IEtlOperationWithEnumerableResult<T> _etlProcess;
        private IEtlProcessBuilder _builder;
        private Action<IEtlProcessBuilder> _bootstrapBuilder;

        protected void Build(Action<IEtlProcessBuilder> builder)
        {
            _bootstrapBuilder = builder;
        }

        public override IEnumerableEtlOperationResult<T> ExecuteWithResult(EtlPipelineContext context)
        {
            _builder = EtlProcessBuilder.Create(context);
            _bootstrapBuilder(_builder);

            _etlProcess = ((EtlProcessBuilder)_builder).Build<T>();
            Named(_etlProcess.Name);

            return _etlProcess.ExecuteWithResult(context);
        }
    }
}