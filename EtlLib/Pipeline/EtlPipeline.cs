using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using EtlLib.Logging;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public sealed class EtlPipeline : IEtlPipeline
    {
        private const string LoggerName = "EtlLib.EtlPipeline";

        private readonly EtlPipelineContext _context;
        private readonly EtlPipelineSettings _settings;
        private readonly List<IEtlOperation> _steps;
        private readonly ILogger _log;
        private readonly Dictionary<IEtlOperation, IEtlOperationResult> _executionResults;

        public string Name { get; }
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
            PrintHeader();

            if (_settings.ObjectPoolRegistrations.Count > 0)
            {
                _log.Info("Initializing object pools...");
                foreach (var pool in _settings.ObjectPoolRegistrations)
                {
                    _log.Info($" * Creating pool for '{pool.Type.Name}' (InitialSize={pool.InitialSize}, AutoGrow={pool.AutoGrow})");
                    _context.ObjectPool.RegisterAndInitializeObjectPool(pool.Type, pool.InitialSize, pool.AutoGrow);
                }
            }

            for (var i = 0; i < _steps.Count; i++)
            {
                _log.Info($"Executing step #{i} ({_steps[i].GetType().Name}): '{_steps[i].Name}'");
                var result = _steps[i].Execute();
                LastResult = result;

                _executionResults[_steps[i]] = result;

                if (_steps[i] is IDisposable disposable)
                {
                    _log.Debug("Disposing of resources used by step.");
                    disposable.Dispose();
                }

                _log.Debug("Performing garbage collection of all generations.");
                GC.Collect();
            }

            _log.Debug("Deallocating all object pools:");
            foreach (var pool in _context.ObjectPool.Pools)
            {
                _log.Debug($" * ObjectPool<{pool.Type.Name}> => Referenced: {pool.Referenced}, Free: {pool.Free}");
            }
            _context.ObjectPool.DeAllocate();

            return null;
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

        private IEtlPipeline RegisterOperation(IEtlOperation operation)
        {
            operation.SetContext(_context);
            _steps.Add(operation);
            return this;
        }

        private IEtlPipelineEnumerableResultContext<TOut> RegisterOperation<TOut>(IEtlOperationWithEnumerableResult<TOut> operation)
        {
            operation.SetContext(_context);
            _steps.Add(operation);
            return new EtlPipelineEnumerableResultContext<TOut>(this, _context);
        }

        private IEtlPipelineWithScalarResultContext<TOut> RegisterOperation<TOut>(IEtlOperationWithScalarResult<TOut> operation)
        {
            operation.SetContext(_context);
            _steps.Add(operation);
            return new EtlPipelineWithScalarResultContext<TOut>(this, _context);
        }

        public static IEtlPipeline Create(Action<EtlPipelineSettings> cfg)
        {
            var settings = new EtlPipelineSettings();
            cfg(settings);

            var context = new EtlPipelineContext();
            settings.ContextInitializer(context);

            return new EtlPipeline(settings, context);
        }
    }

    public interface IEtlPipelineWithScalarResultContext<out TOut>
    {
        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, TOut> result);
    }

    public class EtlPipelineWithScalarResultContext<TOut> : IEtlPipelineWithScalarResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

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
        IEtlPipeline SaveResult(string stateKeyName);
        IEtlPipeline WithResult(Action<EtlPipelineContext, IEnumerable<TOut>> result);
        IEtlPipeline ForEachResult(Action<EtlPipelineContext, int, TOut> result);
    }

    public class EtlPipelineEnumerableResultContext<TOut> : IEtlPipelineEnumerableResultContext<TOut>
    {
        private readonly EtlPipeline _parentPipeline;
        private readonly EtlPipelineContext _context;

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

    public class DynamicInvokeEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly Delegate _action;
        private readonly object[] _args;

        public DynamicInvokeEtlOperation(Delegate action, params object[] args)
        {
            _action = action;
            _args = args;
        }

        public override IEtlOperationResult Execute()
        {
            try
            {
                _action.DynamicInvoke(_args);
            }
            catch (Exception ex)
            {
                if (EtlLibConfig.EnableDebug)
                    Debugger.Break();

                return new EtlOperationResult(false)
                    .WithError(this, ex);
            }
            
            return new EtlOperationResult(true);
        }
    }


}