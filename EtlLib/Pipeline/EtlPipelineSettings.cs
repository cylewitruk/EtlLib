using System;
using System.Collections.Generic;

namespace EtlLib.Pipeline
{
    public class EtlPipelineSettings
    {
        public string Name { get; set; }
        public Action<EtlPipelineContext> ContextInitializer { get; set; }
        public List<ObjectPoolSettings> ObjectPoolRegistrations { get; }

        public EtlPipelineSettings()
        {
            ContextInitializer = context => { };
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

        public EtlPipelineSettings RegisterObjectPool<T>(int initialSize = 5000, bool autoGrow = true)
        {
            ObjectPoolRegistrations.Add(new ObjectPoolSettings(typeof(T), initialSize, autoGrow));
            return this;
        }
    }
}