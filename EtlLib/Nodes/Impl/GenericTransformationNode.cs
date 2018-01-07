using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericTransformationNode<T> : AbstractInputOutputNode<T, T>
        where T : class, IFreezable
    {
        private readonly Func<IDictionary<string, object>, T, T> _transform;
        private readonly ConcurrentDictionary<string, object> _stateDictionary;

        public GenericTransformationNode(Func<IDictionary<string, object>, T, T> transform)
        {
            _transform = transform;
            _stateDictionary = new ConcurrentDictionary<string, object>();
        }

        public override void Execute()
        {
            foreach (var item in Input)
            {
                Emit(_transform(_stateDictionary, item));
            }

            Emitter.SignalEnd();
        }
    }

    public class GenericTransformationNode<T, TState> : AbstractInputOutputNode<T, T>
        where T : class, IFreezable
        where TState : new()
    {
        private readonly Func<TState, T, T> _transform;
        private readonly TState _state;

        public GenericTransformationNode(Func<TState, T, T> transform)
        {
            _transform = transform;
            _state = new TState();
        }

        public override void Execute()
        {
            foreach (var item in Input)
            {
                Emit(_transform(_state, item));
            }

            Emitter.SignalEnd();
        }
    }
}