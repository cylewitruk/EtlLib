using System;
using System.Threading.Tasks;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public abstract class Node : INode
    {
        public Guid Id { private set; get; }
        public EtlProcessContext Context { private set; get; }

        public INode SetId(Guid id)
        {
            Id = id;
            return this;
        }

        public INode SetContext(EtlProcessContext context)
        {
            Context = context;
            return this;
        }

        public abstract void Execute();

        public override string ToString()
        {
            return $"{GetType().Name}=({Id})";
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        protected bool Equals(Node other)
        {
            return Id.Equals(other.Id);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }
}