using System;
using System.Collections.Generic;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.UnitTests.EtlPipelineTests
{
    public class ActionEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Func<EtlPipelineContext, bool> _action;
        private readonly List<EtlOperationError> _errors;

        public ActionEtlOperation(Func<EtlPipelineContext, bool> action)
        {
            _action = action;
            _errors = new List<EtlOperationError>();
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            return new EtlOperationResult(_action(context))
                .WithErrors(_errors);
        }

        public ActionEtlOperation WithErrors(params EtlOperationError[] errors)
        {
            _errors.AddRange(errors);
            return this;
        }
    }
}