using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes
{
    public interface INode
    {
        Guid Id { get; }
        EtlProcessContext Context { get; }

        INode SetId(Guid id);
        INode SetContext(EtlProcessContext context);

        Task Execute();
    }

    public interface INodeWithInput : INode { }

    public interface INodeWithInput<TIn> : INodeWithInput
        where TIn : class, IFreezable
    {
        IEnumerable<TIn> Input { get; }

        INodeWithInput<TIn> SetInput(IEnumerable<TIn> input);
    }

    public interface INodeWithInput2<TIn> : INodeWithInput<TIn>
        where TIn : class, IFreezable
    {
        IEnumerable<TIn> Input2 { get; }

        INodeWithInput2<TIn> SetInput2(IEnumerable<TIn> input2);
    }

    public interface INodeWithOutput : INode { }

    public interface INodeWithOutput<TOut> : INodeWithOutput
        where TOut : class, IFreezable
    {
        IEmitter<TOut> Emitter { get; }

        INodeWithOutput<TOut> SetEmitter(IEmitter<TOut> emitter);
    }

    public interface INodeWithInputOutput<TIn, TOut> : INodeWithInput<TIn>, INodeWithOutput<TOut>
        where TIn : class, IFreezable
        where TOut : class, IFreezable
    {
    }
}