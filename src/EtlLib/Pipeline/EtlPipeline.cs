using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using EtlLib.Logging;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public sealed class EtlPipeline : IEtlPipeline, IEtlOperation
    {
        private const string LoggerName = "EtlLib.EtlPipeline";

        private readonly EtlPipelineContext _context;
        private readonly EtlPipelineSettings _settings;
        private readonly List<IEtlOperation> _steps;
        private readonly ILogger _log;
        private readonly Dictionary<IEtlOperation, IEtlOperationResult> _executionResults;

        private Func<EtlPipelineContext, bool> _currentPredicate;

        public string Name { get; private set; }

        IEtlOperation IEtlOperation.Named(string name)
        {
            Name = name;
            return this;
        }

        IEtlOperationResult IEtlOperation.Execute(EtlPipelineContext context)
        {
            return InternalExecute(context);
        }

        public EtlPipelineContext Context => _context;
        public IEtlOperationResult LastResult { get; private set; }

        private EtlPipeline(EtlPipelineSettings settings, EtlPipelineContext context)
        {
            _context = context;
            Name = settings.Name;

            _log = context.GetLogger(LoggerName);
            _steps = new List<IEtlOperation>();
            _executionResults = new Dictionary<IEtlOperation, IEtlOperationResult>();

            _settings = settings;
        }

        public EtlPipelineResult Execute()
        {
            return InternalExecute(Context);
        }

        private EtlPipelineResult InternalExecute(EtlPipelineContext context)
        {
            PrintHeader();

            if (_settings.ObjectPoolRegistrations.Count > 0)
            {
                _log.Info("Initializing object pools...");
                foreach (var pool in _settings.ObjectPoolRegistrations)
                {
                    _log.Info($" * Creating pool for '{pool.Type.Name}' (InitialSize={pool.InitialSize}, AutoGrow={pool.AutoGrow})");
                    context.ObjectPool.RegisterAndInitializeObjectPool(pool.Type, pool.InitialSize, pool.AutoGrow);
                }
            }

            for (var i = 0; i < _steps.Count; i++)
            {
                _log.Info($"Executing step #{i + 1} ({_steps[i].GetType().Name}): '{_steps[i].Name}'");

                var operation = _steps[i];

                try
                {
                    _executionResults[_steps[i]] = LastResult = ExecuteOperation(operation, context);
                }
                catch (PipelineAbortException e)
                {
                    _log.Warn($"Error handling has indicated that the ETL process should be halted after error, terminating.  Message: {e.Message}");
                    break;
                }
            }

            _log.Debug("Deallocating all object pools:");
            foreach (var pool in _context.ObjectPool.Pools)
            {
                _log.Debug($" * ObjectPool<{pool.Type.Name}> => Referenced: {pool.Referenced}, Free: {pool.Free}");
            }
            context.ObjectPool.DeAllocate();

            return null;
        }

        private IEtlOperationResult ExecuteOperation(IEtlOperation operation, EtlPipelineContext context)
        {
            IEtlOperationResult result = null;
            try
            {
                if (operation is IEtlOperationCollection collection)
                {
                    var multiResult = new EtlOperationResult(true);
                    foreach (var op in collection.GetOperations())
                    {
                        _executionResults[op] = LastResult = ExecuteOperation(op, context);
                        multiResult
                            .WithErrors(LastResult.Errors)
                            .QuiesceSuccess(LastResult.IsSuccess);
                    }

                    return multiResult;
                }
                else
                {
                    result = operation.Execute(context);
                }
            }
            catch (Exception e)
            {
                if (EtlLibConfig.EnableDebug)
                    Debugger.Break();

                _log.Error(
                    $"An error occured while executing operation '{operation.Name}' ({operation.GetType().Name}): {e}");
                var error = new EtlOperationError(operation, e);
                if (!_settings.OnErrorFn.Invoke(context, new[] {error}))
                {
                    throw new PipelineAbortException(error);
                }
            }
            finally
            {
                if (result?.Errors.Count > 0)
                {
                    context.ReportErrors(result.Errors);
                    if (!_settings.OnErrorFn.Invoke(context, result.Errors.ToArray()))
                    {
                        throw new PipelineAbortException(result.Errors);
                    }
                }

                if (operation is IDisposable disposable)
                {
                    _log.Debug("Disposing of resources used by step.");
                    disposable.Dispose();
                }

                _log.Debug("Cleaning up (globally).");
                GC.Collect();
            }

            return result;
        }

        private void PrintHeader()
        {
            _log.Info(new string('#', 80));
            _log.Info($"# ETL Pipeline '{Name}'");
            _log.Info($"# Steps to Execute: {_steps.Count}");
            foreach (var step in _steps)
            {
                _log.Info($"#    {step.Name}");
            }
            _log.Info($"# Start Time: {DateTime.Now}");
            _log.Info(new string('#', 80));
        }

        public IEtlPipeline Run(Action<EtlPipelineContext, IEtlProcessBuilder> builder)
        {
            var b = EtlProcessBuilder.Create();
            builder(_context, b);

            return this;
        }

        public IEtlPipeline Run(IEtlOperation operation)
        {
            return RegisterOperation(operation);
        }

        public IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx)
        {
            return RegisterOperation(ctx(_context));
        }

        public IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithEnumerableResult<TOut> operation)
        {
            return RegisterOperation(operation);
        }

        public IEtlPipeline Run<TOut>(
            Func<EtlPipelineContext, IEtlOperationWithEnumerableResult<TOut>> ctx,
            Action<IEtlPipelineEnumerableResultContext<TOut>> result)
        {
            var op = ctx(_context);
            var ret = RegisterOperation(op);
            result(ret);
            return this;
        }

        public IEtlPipeline Run<TOut>(IEtlOperationWithEnumerableResult<TOut> operation, Action<IEtlPipelineEnumerableResultContext<TOut>> result)
        {
            var ret = RegisterOperation(operation);
            result(ret);
            return this;
        }

        public IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(
            Func<EtlPipelineContext, IEtlOperationWithEnumerableResult<TOut>> operation)
        {
            return RegisterOperation(operation(_context));
        }

        public IEtlPipeline Run<TOut>(IEtlOperationWithScalarResult<TOut> operation, Action<IEtlPipelineWithScalarResultContext<TOut>> result)
        {
            var ret = RegisterOperation(operation);
            result(ret);
            return this;
        }

        public IEtlPipeline Run<TOut>(
            Func<EtlPipelineContext, IEtlOperationWithScalarResult<TOut>> ctx,
            Action<IEtlPipelineWithScalarResultContext<TOut>> result)
        {
            var op = ctx(_context);
            var ret = RegisterOperation(op);
            result(ret);
            return this;
        }

        public IEtlPipelineWithScalarResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithScalarResult<TOut> operation)
        {
            return RegisterOperation(operation);
        }

        public IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlOperation>> ctx)
        {
            var operations = ctx(_context).ToArray();
            var parellelOperation =
                new ParallelOperation(
                    $"Executing steps in parellel => [{string.Join(", ", operations.Select(x => x.Name))}]",
                    operations);

            return RegisterOperation(parellelOperation);
        }

        public IEtlPipeline RunIf(Func<EtlPipelineContext, bool> predicate, Func<EtlPipelineContext, IEtlOperation> operation)
        {
            var method = new Action(() =>
            {
                if (!predicate(Context))
                    return;

                var op = operation(Context);
                op.Execute(Context);
            });

            Run(new DynamicInvokeEtlOperation(method).Named("Conditional Execution"));
            return this;
        }

        public IEtlPipeline If(Func<EtlPipelineContext, bool> predicate, Action<IEtlPipeline> pipeline)
        {
            _currentPredicate = predicate;
            pipeline(this);
            _currentPredicate = null;
            return this;
        }

        private IEtlPipeline RegisterOperation(IEtlOperation operation)
        {
            
            AddOperation(operation);
            return this;
        }

        private IEtlPipelineEnumerableResultContext<TOut> RegisterOperation<TOut>(IEtlOperationWithEnumerableResult<TOut> operation)
        {
            AddOperation(operation);
            return new EtlPipelineEnumerableResultContext<TOut>(this, _context);
        }

        private IEtlPipelineWithScalarResultContext<TOut> RegisterOperation<TOut>(IEtlOperationWithScalarResult<TOut> operation)
        {
            AddOperation(operation);
            return new EtlPipelineWithScalarResultContext<TOut>(this, _context);
        }

        private void AddOperation(IEtlOperation operation)
        {
            if (_currentPredicate != null)
            {
                var conditionalOperation = new ConditionalEtlOperation(_currentPredicate, operation);
                _steps.Add(conditionalOperation);
            }
            else
            {
                _steps.Add(operation);
            }
        }

        public static IEtlPipeline Create(Action<EtlPipelineSettings> settings)
        {
            var s = new EtlPipelineSettings();
            settings(s);

            var config = new EtlPipelineConfig();
            s.ConfigInitializer(config);

            var context = s.ExistingContext ?? new EtlPipelineContext(config);
            s.ContextInitializer(context);

            return new EtlPipeline(s, context);
        }

        public IEnumerable<IEtlOperation> GetOperations() => _steps;
    }

    public interface IEtlPipelineWithScalarResultContext<out TOut>
    {
        IEtlPipeline Pipeline { get; }

        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, TOut> result);
    }

    public class EtlPipelineWithScalarResultContext<TOut> : IEtlPipelineWithScalarResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

        public IEtlPipeline Pipeline => _parentPipeline;

        public EtlPipelineWithScalarResultContext(EtlPipeline pipeline, EtlPipelineContext context)
        {
            _parentPipeline = pipeline;
            _context = context;
        }

        public IEtlPipeline SaveResult(string stateKeyName)
        {
            var method = new Action(() =>
            {
                var result = (IScalarEtlOperationResult<TOut>)_parentPipeline.LastResult;
                _context.State[stateKeyName] = result.Result;
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("Save Scalar Result"));
        }

        public IEtlPipeline WithResult(Action<EtlPipelineContext, TOut> result)
        {
            var method = new Action(() =>
            {
                var value = ((IScalarEtlOperationResult<TOut>) _parentPipeline.LastResult).Result;
                result(_context, value);
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("With Scalar Result"));
        }
    }

    public interface IEtlPipelineEnumerableResultContext<out TOut>
    {
        IEtlPipeline Pipeline { get; }

        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, IEnumerable<TOut>> result);
        IEtlPipeline ForEachResult(Action<EtlPipelineContext, int, TOut> result);
    }

    public class EtlPipelineEnumerableResultContext<TOut> : IEtlPipelineEnumerableResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

        public IEtlPipeline Pipeline => _parentPipeline;

        public EtlPipelineEnumerableResultContext(EtlPipeline pipeline, EtlPipelineContext context)
        {
            _parentPipeline = pipeline;
            _context = context;
        }

        public IEtlPipeline SaveResult(string stateKeyName)
        {
            var method = new Action(() =>
            {
                var result = (IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult;
                _context.State[stateKeyName] = result.Result;
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("Save Enumerable Result"));
        }

        public IEtlPipeline WithResult(Action<EtlPipelineContext, IEnumerable<TOut>> result)
        {
            var method = new Action(() =>
            {
                var value = ((IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult).Result;
                result(_context, value);
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named("With Enumerable Result"));
        }

        public IEtlPipeline ForEachResult(Action<EtlPipelineContext, int, TOut> result)
        {
            var method = new Action(() =>
            {
                var results = ((IEnumerableEtlOperationResult<TOut>)_parentPipeline.LastResult).Result;

                var count = 0;
                foreach (var item in results)
                {
                    result(_context, ++count, item);
                }
            });

            return _parentPipeline.Run(new DynamicInvokeEtlOperation(method).Named($"Foreach {typeof(TOut).Name} in Result"));
        }
    }

    public interface IEtlOperationCollection
    {
        IEnumerable<IEtlOperation> GetOperations();
    }

    public class EtlOperationGroup : IEtlOperationCollection
    {
        private readonly List<IEtlOperation> _operations;

        public EtlOperationGroup(params IEtlOperation[] operations)
        {
            _operations = new List<IEtlOperation>(operations);
        }

        public EtlOperationGroup AddOperation(IEtlOperation operation)
        {
            _operations.Add(operation);
            return this;
        }

        public IEnumerable<IEtlOperation> GetOperations() => _operations;
    }
}