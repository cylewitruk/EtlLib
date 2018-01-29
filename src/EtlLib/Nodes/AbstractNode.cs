using System;
using EtlLib.Data;
using EtlLib.Pipeline;
using EtlLib.Support;

namespace EtlLib.Nodes
{
    public abstract class AbstractNode : INode
    {
        public Guid Id { get; private set; }
        public INodeWaiter Waiter { get; private set; }
        public IErrorHandler ErrorHandler { get; private set; }

        public INode SetId(Guid id)
        {
            Id = id;
            return this;
        }

        public INode SetWaiter(INodeWaiter waiter)
        {
            Waiter = waiter;
            return this;
        }

        public INode SetErrorHandler(IErrorHandler errorHandler)
        {
            ErrorHandler = errorHandler;
            return this;
        }

        protected void RaiseError(Exception e)
        {
            ErrorHandler?.RaiseError(this, e);
        }

        protected void RaiseError(Exception e, INodeOutput item)
        {
            ErrorHandler?.RaiseError(this, e, item);
        }

        public void Execute(EtlPipelineContext context)
        {
            Waiter?.Wait();

            try
            {
                OnExecute(context);
            }
            catch (Exception e)
            {
                RaiseError(e);

                if (this is INodeWithOutput output)
                    output.Emitter.SignalEnd();
            }
        }

        public abstract void OnExecute(EtlPipelineContext context);

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