using System;
using System.Collections.Generic;

namespace EtlLib.Pipeline.Operations
{
    public interface IEtlOperationResult
    {
        bool IsSuccess { get; }
        IReadOnlyCollection<EtlOperationError> Errors { get; }
    }

    public interface IEnumerableEtlOperationResult<out T> : IEtlOperationResult, IDisposable
    {
        IEnumerable<T> Result { get; }
    }

    public interface IScalarEtlOperationResult<out T> : IEtlOperationResult
    {
        T Result { get; }
    }

    public class EtlOperationResult : IEtlOperationResult
    {
        private readonly List<EtlOperationError> _errors;

        public bool IsSuccess { get; }
        public IReadOnlyCollection<EtlOperationError> Errors => _errors;

        public EtlOperationResult(bool isSuccess)
        {
            IsSuccess = isSuccess;
            _errors = new List<EtlOperationError>();
        }

        public EtlOperationResult WithErrors(IEnumerable<EtlOperationError> errors)
        {
            _errors.AddRange(errors);
            return this;
        }

        public EtlOperationResult WithError(EtlOperationError error)
        {
            _errors.Add(error);
            return this;
        }

        public EtlOperationResult WithError(IEtlOperation sourceOperation, Exception exception,
            object sourceItem)
        {
            _errors.Add(new EtlOperationError(sourceOperation, exception, sourceItem));
            return this;
        }

        public EtlOperationResult WithError(IEtlOperation sourceOperation, Exception exception)
        {
            _errors.Add(new EtlOperationError(sourceOperation, exception));
            return this;
        }
    }

    public class EnumerableEtlOperationResult<T> : EtlOperationResult,
        IEnumerableEtlOperationResult<T>
    {
        public IEnumerable<T> Result { get; private set; }

        public EnumerableEtlOperationResult(bool isSuccess, IEnumerable<T> result) : base(isSuccess)
        {
            Result = result;
        }

        public void Dispose()
        {
            Result = null;
        }
    }

    public class ScalarEtlOperationResult<T> : EtlOperationResult, IScalarEtlOperationResult<T>
    {
        public T Result { get; }

        public ScalarEtlOperationResult(bool isSuccess, T result) : base(isSuccess)
        {
            Result = result;
        }
    }
}