using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlOperationCollection
    {
        IEnumerable<IEtlOperation> GetOperations();
    }
}