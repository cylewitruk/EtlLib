using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Operations;
using EtlLib.Support;

namespace EtlLib.Pipeline
{
    public class EtlPipelineSettings
    {
        public string Name { get; set; }
        public Action<EtlPipelineContext> ContextInitializer { get; set; }
        public Action<EtlPipelineConfig> ConfigInitializer { get; set; }
        public List<ObjectPoolSettings> ObjectPoolRegistrations { get; }
        public bool ThrowOnError { get; set; }
        public Action<EtlPipelineContext, IEtlOperation, Exception> OnError { get; set; }
        public EtlPipelineContext ExistingContext { get; set; }

        public EtlPipelineSettings()
        {
            ContextInitializer = context => { };
            ConfigInitializer = config => { };
            ObjectPoolRegistrations = new List<ObjectPoolSettings>();
        }

        public EtlPipelineSettings Named(string name)
        {
            Name = name;
            return this;
        }

        public EtlPipelineSettings WithContextInitializer(Action<EtlPipelineContext> ctx)
        {
            ContextInitializer = ctx;
            return this;
        }

        public EtlPipelineSettings WithConfig(Action<EtlPipelineConfig> cfg)
        {
            ConfigInitializer = cfg;
            return this;
        }

        public EtlPipelineSettings RegisterObjectPool<T>(int initialSize = 5000, bool autoGrow = true)
        {
            ObjectPoolRegistrations.Add(new ObjectPoolSettings(typeof(T), initialSize, autoGrow));
            return this;
        }

        public EtlPipelineSettings ThrowOnException()
        {
            ThrowOnError = true;
            return this;
        }

        public EtlPipelineSettings OnException(Action<EtlPipelineContext, IEtlOperation, Exception> err)
        {
            OnError = err;
            return this;
        }

        public EtlPipelineSettings UseExistingContext(EtlPipelineContext context)
        {
            ExistingContext = context;
            return this;
        }
    }
}