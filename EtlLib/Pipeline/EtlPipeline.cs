using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EtlLib.Logging;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Pipeline
{
    public class EtlPipeline : IEtlPipeline
    {
        private const string LoggerName = "EtlLib.EtlPipeline";

        private readonly EtlPipelineContext _context;
        private readonly EtlPipelineSettings _settings;
        private readonly List<IExecutableNode> _steps;
        private readonly ILogger _log;
        private readonly ILoggingAdapter _loggingAdapter;

        public string Name { get; }

        private EtlPipeline(EtlPipelineSettings settings, EtlPipelineContext context)
        {
            _loggingAdapter = settings.LoggingAdapter;
            _context = context;
            Name = settings.Name;

            _log = _loggingAdapter.CreateLogger(LoggerName);
            _steps = new List<IExecutableNode>();

            _settings = settings;
        }

        public PipelineResult Execute()
        {
            PrintHeader();

            if (_settings.ObjectPoolRegistrations.Count > 0)
            {
                _log.Info("Initializing object pools...");
                foreach (var pool in _settings.ObjectPoolRegistrations)
                {
                    _log.Info($" - ObjectPool<{pool.Type.Name}> (InitialSize={pool.InitialSize}, AutoGrow={pool.AutoGrow})");
                    _context.ObjectPool.RegisterAndInitializeObjectPool(pool.Type, pool.InitialSize, pool.AutoGrow);
                }
            }

            for (var i = 0; i < _steps.Count; i++)
            {
                _log.Info($"Executing step #{i}: '{_steps[i].Name}'");
                _steps[i].Execute();

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

        public IEtlPipeline Run(Action<EtlPipelineContext, EtlProcessSettings> settings, Action<EtlPipelineContext, IEtlProcessBuilder> builder)
        {
            var s = new EtlProcessSettings()
            {
                LoggingAdapter = _loggingAdapter,
                Name = $"Step {_steps.Count + 1}",
                ContextInitializer = cfg => { }
            };
            
            var b = EtlProcessBuilder.Create(processSettings => settings(_context, s));
            builder(_context, b);

            return this;
        }

        public IEtlPipeline Run(IExecutableNode executable)
        {
            _steps.Add(executable);
            return this;
        }

        public IEtlPipeline Run(Func<EtlPipelineContext, IExecutableNode> ctx)
        {
            _steps.Add(ctx(_context));
            return this;
        }

        public IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IExecutableNode>> ctx)
        {
            var executables = ctx(_context).ToArray();
            _steps.Add(new ParallelExecutable($"Executing steps in parellel => [{string.Join(", ", executables.Select(x => x.Name))}]", executables));
            return this;
        }

        public static IEtlPipeline Create(Action<EtlPipelineSettings> cfg)
        {
            var settings = new EtlPipelineSettings();
            cfg(settings);

            var context = new EtlPipelineContext(settings.LoggingAdapter);
            settings.ContextInitializer(context);

            return new EtlPipeline(settings, context);
        }
    }

    public class ParallelExecutable : IExecutableNode
    {
        private readonly List<IExecutableNode> _steps;

        public string Name { get; }

        public ParallelExecutable(string name, params IExecutableNode[] executables)
        {
            _steps = new List<IExecutableNode>(executables);
            Name = name;
        }
        
        public void Execute()
        {
            var tasks = new ConcurrentBag<Task>();

            foreach (var step in _steps)
            {
                var task = Task.Run(() => step.Execute());
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
        }
    }
}