using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Support
{
    public interface IInputOutputAdapter : IDisposable
    {
        INode OutputNode { get; }
        IEnumerable<INode> AttachedNodes { get; }
        int EmitCount { get; }

        bool AttachConsumer(INodeWithInput input);

        IInputOutputAdapter SetObjectPool(IObjectPool pool);
        IInputOutputAdapter SetNodeStatisticsCollector(NodeStatistics stats);
    }

    public interface IInputOutputAdapter<T> : IInputOutputAdapter
        where T : class, INodeOutput<T>, new()
    {
        bool AttachConsumer(INodeWithInput<T> input);
    }

    public class InputOutputAdapter<T> : IInputOutputAdapter<T>, IEmitter<T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly ConcurrentDictionary<INode, BlockingCollection<T>> _queueMap;
        private readonly ConcurrentBag<INodeWithInput<T>> _inputs;
        private readonly INodeWithOutput<T> _output;
        private NodeStatistics _nodeStatistics;
        private ObjectPool<T> _objectPool;
        private ILogger _log;
        private volatile int _emittedItems;

        public INode OutputNode => _output;
        public INodeWaitSignaller WaitSignaller { get; }
        public INodeWaiter Waiter { get; }
        public int EmitCount => _emittedItems;
        public IEnumerable<INode> AttachedNodes => _inputs;

        public InputOutputAdapter(INodeWithOutput<T> output)
        {
            _queueMap = new ConcurrentDictionary<INode, BlockingCollection<T>>();
            _inputs = new ConcurrentBag<INodeWithInput<T>>();
            _output = output;
            _log = EtlLibConfig.LoggingAdapter.CreateLogger("EtlLib.IOAdapter");

            if (output is IBlockingNode)
            {
                var signaller = new BlockingWaitSignaller();
                ((IBlockingNode)output).SetWaitSignaller(WaitSignaller);
                Waiter = signaller;
                WaitSignaller = signaller;
            }
            else
            {
                Waiter = new NoWaitNodeWaiter();
            }

            _output.SetEmitter(this);
        }

        public IInputOutputAdapter SetNodeStatisticsCollector(NodeStatistics stats)
        {
            _nodeStatistics = stats;
            return this;
        }

        public IInputOutputAdapter SetObjectPool(IObjectPool pool)
        {
            _objectPool = (ObjectPool<T>)pool;
            return this;
        }

        public InputOutputAdapter<T> WithLogger(ILogger log)
        {
            _log = log;
            return this;
        }

        public IEnumerable<T> GetConsumingEnumerable(INode node)
        {
            return _queueMap[node].GetConsumingEnumerable();
        }

        public IEnumerable<T> GetConsumingEnumerable(INodeWithInput<T> node)
        {
            return _queueMap[node].GetConsumingEnumerable();
        }

        public bool AttachConsumer(INodeWithInput input)
        {
            return AttachConsumer((INodeWithInput<T>) input);
        }

        public bool AttachConsumer(INodeWithInput<T> input)
        {
            if (input.Input != null)
                throw new InvalidOperationException($"Node (Id={input.Id}, Type={input.GetType().Name}) already has an input assigned.");

            if (!_queueMap.TryAdd(input, new BlockingCollection<T>(new ConcurrentQueue<T>())))
                return false;

            input
                .SetInput(GetConsumingEnumerable(input))
                .SetWaiter(Waiter);

            _inputs.Add(input);

            return true;
        }

        public void Emit(T item)
        {
            if (_emittedItems == 0)
                _log.Debug($"Node {_output} emitting its first item.");

            item.Freeze();
            _emittedItems++;

            _nodeStatistics?.IncrementWrites(OutputNode);

            var firstTarget = true;
            foreach (var queue in _queueMap)
            {
                if (firstTarget)
                {
                    queue.Value.Add(item);
                    firstTarget = false;
                }
                else
                {
                    var duplicatedItem = _objectPool?.Borrow() ?? new T();
                    item.CopyTo(duplicatedItem);
                    queue.Value.Add(duplicatedItem);
                }
                _nodeStatistics?.IncrementReads(queue.Key);
            }

            if (_emittedItems % 5000 == 0)
                _log.Debug($"Node {_output} has emitted {_emittedItems} items.");
        }

        public void SignalEnd()
        {
            _log.Debug($"Node {_output} has signalled the end of its data stream (emitted {_emittedItems} total items).");

            foreach (var queue in _queueMap.Values)
                queue.CompleteAdding();
        }

        public void Dispose()
        {
            foreach(var buffer in _queueMap)
                buffer.Value.Dispose();
            _queueMap.Clear();
        }
    }
}