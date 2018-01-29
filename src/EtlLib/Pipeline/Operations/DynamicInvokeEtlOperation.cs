using System;
using System.Diagnostics;

namespace EtlLib.Pipeline.Operations
{
    public class DynamicInvokeEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Delegate _action;
        private readonly object[] _args;

        public DynamicInvokeEtlOperation(Delegate action, params object[] args)
        {
            _action = action;
            _args = args;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            try
            {
                _action.DynamicInvoke(_args);
            }
            catch (Exception ex)
            {
                if (EtlLibConfig.EnableDebug)
                    Debugger.Break();

                return new EtlOperationResult(false)
                    .WithError(this, ex);
            }
            
            return new EtlOperationResult(true);
        }
    }
}