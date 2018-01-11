﻿using System;
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
        private readonly List<IExecutable> _steps;
        private readonly ILogger _log;
        private readonly ILoggingAdapter _loggingAdapter;

        public string Name { get; }

        private EtlPipeline(EtlPipelineSettings settings, EtlPipelineContext context)
        {
            _loggingAdapter = settings.LoggingAdapter;
            _context = context;
            Name = settings.Name;

            _log = _loggingAdapter.CreateLogger(LoggerName);
            _steps = new List<IExecutable>();
        }

        public PipelineResult Execute()
        {
            PrintHeader();
            for (var i = 0; i < _steps.Count; i++)
            {
                _log.Info($"Executing step #{i}: '{_steps[i].Name}'");
                _steps[0].Execute();
            }

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

        public IEtlPipeline Run(Action<EtlProcessSettings> settings, Action<IEtlProcessBuilder> builder)
        {
            var s = new EtlProcessSettings()
            {
                LoggingAdapter = _loggingAdapter,
                Name = $"Step {_steps.Count + 1}",
                ContextInitializer = cfg => { }
            };
            
            var b = EtlProcessBuilder.Create(processSettings => settings(s));
            builder(b);

            return this;
        }

        public IEtlPipeline Run(IExecutable executable)
        {
            _steps.Add(executable);
            return this;
        }

        public IEtlPipeline RunParallel(params IExecutable[] executables)
        {
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

    public class ParallelExecutable : IExecutable
    {
        private readonly List<IExecutable> _steps;

        public string Name { get; }

        public ParallelExecutable(string name, params IExecutable[] executables)
        {
            _steps = new List<IExecutable>(executables);
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