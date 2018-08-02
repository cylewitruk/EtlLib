using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericTransformationNode<T> : AbstractProcessingNode<T, T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly Func<IDictionary<string, object>, T, T> _transform;
        private readonly ConcurrentDictionary<string, object> _stateDictionary;

        public GenericTransformationNode(Func<IDictionary<string, object>, T, T> transform)
        {
            _transform = transform;
            _stateDictionary = new ConcurrentDictionary<string, object>();
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                try
                {
                    Emit(_transform(_stateDictionary, item));
                }
                catch (Exception e)
                {
                    RaiseError(e, item);
                }
            }

            TypedEmitter.SignalEnd();
        }
    }

    public class GenericTransformationNode<T, TState> : AbstractProcessingNode<T, T>
        where T : class, INodeOutput<T>, new()
        where TState : new()
    {
        private readonly Func<TState, T, T> _transform;
        private readonly TState _state;

        public GenericTransformationNode(Func<TState, T, T> transform)
        {
            _transform = transform;
            _state = new TState();
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                try
                {
                    Emit(_transform(_state, item));
                }
                catch (Exception e)
                {
                    RaiseError(e, item);
                }
            }

            TypedEmitter.SignalEnd();
        }
    }
}