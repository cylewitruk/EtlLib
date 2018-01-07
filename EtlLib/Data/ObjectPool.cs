using System;
using System.Collections.Generic;

namespace EtlLib.Data
{
    public class ObjectPool<T>
    {
        private readonly bool _autoGrow;
        private bool _matchNonPublicConstructor;
        private volatile int _referenced;
        private readonly Stack<T> _stack;

        #region Properties

        public Func<int, T> CustomCreateInstance { get; set; }

        /// <summary>
        /// Gets whether or not this object pool has been initialized.
        /// </summary>
        public bool IsInitialized { get; private set; }

        /// <summary>
        /// Gets the total size of the pool.
        /// </summary>
        public int Size { get; }

        /// <summary>
        /// Gets the current referenced count.
        /// </summary>
        public int Referenced => _referenced;

        /// <summary>
        /// Gets the current free count.
        /// </summary>
        public int Free => _stack.Count;

        #endregion

        #region Constructor

        public ObjectPool(int size, bool autoGrow = false)
        {
            Size = size;
            _stack = new Stack<T>(size);
            _autoGrow = autoGrow;
        }

        #endregion

        #region Methods [public]

        /// <summary>
        /// Initializes the object pool to the size specified in the constructor.
        /// </summary>
        public void Initialize(bool matchNonPublicConstructor = false)
        {
            if (IsInitialized)
                throw new InvalidOperationException("Object pool is already initialized.");

            _matchNonPublicConstructor = matchNonPublicConstructor;

            lock (_stack)
            {
                for (var i = 0; i < Size; i++)
                {
                    T obj;
                    if (CustomCreateInstance != null)
                        obj = CustomCreateInstance(i);
                    else
                        obj = (T)Activator.CreateInstance(typeof(T), _matchNonPublicConstructor);
                    _stack.Push(obj);
                }

                IsInitialized = true;
            }
        }

        /// <summary>
        /// Borrows an object from the pool.
        /// </summary>
        /// <returns></returns>
        public T Borrow()
        {
            if (!IsInitialized)
                throw new InvalidOperationException("Object pool has not been initialized or has been deallocated.  Call the Initialize() method before trying to use the pool.");

            lock (_stack)
            {
                T obj;
                if (_referenced >= Size && _autoGrow)
                {
                    if (CustomCreateInstance != null)
                        obj = CustomCreateInstance(++_referenced);
                    else
                    {
                        obj = (T)Activator.CreateInstance(typeof(T), _matchNonPublicConstructor);
                        _referenced++;
                    }
                }
                else
                {
                    obj = _stack.Pop();
                    _referenced++;
                }

                return obj;
            }
        }

        /// <summary>
        /// Returns an object to the pool.
        /// </summary>
        public void Return(T obj)
        {
            if (!IsInitialized)
                throw new InvalidOperationException("Object pool has not been initialized or has been deallocated.  Call the Initialize() method before trying to use the pool.");

            lock (_stack)
            {
                _stack.Push(obj);
                _referenced--;
            }
        }

        /// <summary>
        /// Deallocates the pool (releases resources used and marks the pool as not-initialized).
        /// </summary>
        public void DeAllocate()
        {
            lock (_stack)
            {
                _stack.Clear();
                _referenced = 0;
                IsInitialized = false;
            }
        }

        #endregion
    }
}