using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public interface INode
    {
        Guid Id { get; }
        EtlProcessContext Context { get; }
        INodeWaiter Waiter { get; }
        IErrorHandler ErrorHandler { get; }

        INode SetId(Guid id);
        INode SetContext(EtlProcessContext context);
        INode SetWaiter(INodeWaiter waiter);
        INode SetErrorHandler(IErrorHandler errorHandler);

        void Execute();
    }

    public interface INodeWithInput : INode { }

    public interface INodeWithInput<TIn> : INodeWithInput
        where TIn : class, INodeOutput<TIn>, new()
    {
        IEnumerable<TIn> Input { get; }

        INodeWithInput<TIn> SetInput(IEnumerable<TIn> input);
    }

    public interface INodeWithInput2<TIn> : INodeWithInput<TIn>
        where TIn : class, INodeOutput<TIn>, new()
    {
        IEnumerable<TIn> Input2 { get; }

        INodeWithInput2<TIn> SetInput2(IEnumerable<TIn> input2);
    }

    public interface INodeWithOutput : INode { }

    public interface INodeWithOutput<TOut> : INodeWithOutput
        where TOut : class, INodeOutput<TOut>, new()
    {
        IEmitter<TOut> Emitter { get; }

        INodeWithOutput<TOut> SetEmitter(IEmitter<TOut> emitter);
    }

    public interface INodeWithInputOutput<TIn, TOut> : INodeWithInput<TIn>, INodeWithOutput<TOut>
        where TIn : class, INodeOutput<TIn>, new()
        where TOut : class, INodeOutput<TOut>, new()
    {
    }
}