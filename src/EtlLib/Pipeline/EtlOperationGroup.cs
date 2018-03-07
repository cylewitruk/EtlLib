using System.Collections.Generic;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public class EtlOperationGroup : IEtlOperationCollection
    {
        private readonly List<IEtlOperation> _operations;

        public EtlOperationGroup(params IEtlOperation[] operations)
        {
            _operations = new List<IEtlOperation>(operations);
        }

        public EtlOperationGroup AddOperation(IEtlOperation operation)
        {
            _operations.Add(operation);
            return this;
        }

        public IEnumerable<IEtlOperation> GetOperations() => _operations;
    }
}