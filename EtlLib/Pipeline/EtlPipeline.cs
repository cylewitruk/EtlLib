using System;
using System.Collections.Generic;
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
                _log.Info($"Executing step #{i}: '{_steps[i].Name}'");
                var result = _steps[i].Execute();
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

        public IEtlPipeline Run(IEtlOperation executable)
        {
            return RegisterOperation(executable);
        }

        public IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx)
        {
            return RegisterOperation(ctx(_context));
        }

        /*public IEtlPipeline Run<TOut>(EtlProcess<TOut> process, Action<EtlPipelineContext, IEnumerableEtlPipelineOperationResult<TOut>> result) 
            where TOut : class, INodeOutput<TOut>, new()
        {

        }*/

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

        public static IEtlPipeline Create(Action<EtlPipelineSettings> cfg)
        {
            var settings = new EtlPipelineSettings();
            cfg(settings);

            var context = new EtlPipelineContext();
            settings.ContextInitializer(context);

            return new EtlPipeline(settings, context);
        }
    }
}