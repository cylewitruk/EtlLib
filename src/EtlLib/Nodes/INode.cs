using System;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;
using EtlLib.Support;

namespace EtlLib.Nodes
{
    public interface INode
    {
        Guid Id { get; }
        INodeWaiter Waiter { get; }
        IErrorHandler ErrorHandler { get; }

        INode SetId(Guid id);
        INode SetWaiter(INodeWaiter waiter);
        INode SetErrorHandler(IErrorHandler errorHandler);

        void Execute(EtlPipelineContext context);
    }

    public interface ISinkNode : INode { }

    public interface ISinkNode<TIn> : ISinkNode
        where TIn : class, INodeOutput<TIn>, new()
    {
        IEnumerable<TIn> Input { get; }

        ISinkNode<TIn> SetInput(IEnumerable<TIn> input);
    }

    public interface ISinkNode2<TIn> : ISinkNode<TIn>
        where TIn : class, INodeOutput<TIn>, new()
    {
        IEnumerable<TIn> Input2 { get; }

        ISinkNode2<TIn> SetInput2(IEnumerable<TIn> input2);
    }

    public interface ISourceNode : INode
    {
        Type OutputType { get; }
        IEmitter Emitter { get; }
    }

    public interface ISourceNode<TOut> : ISourceNode
        where TOut : class, INodeOutput<TOut>, new()
    {
        IEmitter<TOut> TypedEmitter { get; }

        ISourceNode<TOut> SetEmitter(IEmitter<TOut> emitter);
    }

    public interface IProcessingNode<TIn, TOut> : ISinkNode<TIn>, ISourceNode<TOut>
        where TIn : class, INodeOutput<TIn>, new()
        where TOut : class, INodeOutput<TOut>, new()
    {
    }

    public interface IResultCollectorNode : ISinkNode { }
}