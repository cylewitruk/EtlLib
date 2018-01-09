using System;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public abstract class AbstractNode : INode
    {
        public Guid Id { get; private set; }
        public EtlProcessContext Context { get; private set; }
        public INodeWaiter Waiter { get; private set; }

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

        public INode SetWaiter(INodeWaiter waiter)
        {
            Waiter = waiter;
            return this;
        }

        public void Execute()
        {
            Waiter?.Wait();

            OnExecute();
        }

        public abstract void OnExecute();

        public override string ToString()
        {
            return $"{GetType().Name}=({Id})";
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        protected bool Equals(AbstractNode other)
        {
            return Id.Equals(other.Id);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }
}