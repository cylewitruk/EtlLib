using System.Collections.Generic;
using System.Linq;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class NodeStatistics
    {
        private readonly Dictionary<INode, SingleNodeStatistics> _stats;

        public NodeStatistics()
        {
            _stats = new Dictionary<INode, SingleNodeStatistics>();
        }

        public void IncrementReads(INode node) => _stats[node].IncrementReads();
        public void IncrementWrites(INode node) => _stats[node].IncrementWrites();
        public void IncrementErrors(INode node) => _stats[node].IncrementErrors();
        public void RegisterNode(INode node) => _stats[node] = new SingleNodeStatistics();
        public SingleNodeStatistics GetNodeStatistics(INode node) => _stats[node];

        public long TotalReads => _stats.Values.Sum(x => x.Reads);
        public long TotalWrites => _stats.Values.Sum(x => x.Writes);
        public long TotalErrors => _stats.Values.Sum(x => x.Errors);
        

        public class SingleNodeStatistics
        {
            private volatile uint _reads, _writes, _errors;
            
            public uint Reads => _reads;
            public uint Writes => _writes;
            public uint Errors => _errors;

            public SingleNodeStatistics()
            {
                _reads = _writes = _errors = 0;
            }

            public void IncrementReads()
            {
                _reads++;
            }

            public void IncrementWrites()
            {
                _writes++;
            }

            public void IncrementErrors()
            {
                _errors++;
            }
        }
    }
}