using System;

namespace EtlLib.Pipeline
{
    public interface IConditionalEtlOperation
    {
        Func<EtlPipelineContext, bool> Predicate { get; }
    }
}