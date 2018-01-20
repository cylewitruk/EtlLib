using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using EtlLib.Data;

namespace EtlLib.Support
{
    public class ObjectPoolContainer
    {
        private readonly IDictionary<Type, IObjectPool> _objectPools;

        public IEnumerable<IObjectPool> Pools => _objectPools.Values;

        public ObjectPoolContainer()
        {
            _objectPools = new ConcurrentDictionary<Type, IObjectPool>();
        }

        internal void RegisterAndInitializeObjectPool(Type type, int initialSize, bool autoGrow)
        {
            if (_objectPools.ContainsKey(type))
                return;

            var poolType = typeof(ObjectPool<>);
            Type[] typeArgs = { type };
            var genericPoolType = poolType.MakeGenericType(typeArgs);
            var o = (IObjectPool)Activator.CreateInstance(genericPoolType, initialSize, autoGrow);
            o.Initialize();

            _objectPools.Add(type, o);
        }

        public T Borrow<T>()
            where T : IResettable, new()
        {
            return !_objectPools.TryGetValue(typeof(T), out var pool) 
                ? new T() 
                : ((ObjectPool<T>) pool).Borrow();
        }

        public void Return<T>(T obj)
            where T : IResettable, new()
        {
            if (!_objectPools.TryGetValue(typeof(T), out var pool))
                return;

            ((ObjectPool<T>)pool).Return(obj);
        }

        public void DeAllocate()
        {
            foreach(var pool in _objectPools.Values)
                pool.DeAllocate();
        }
    }
}