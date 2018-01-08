using System;
using System.Collections.Generic;
using EtlLib.Logging;

namespace EtlLib.Pipeline.Builders
{
    public class EtlProcessSettings
    {
        public ILoggingAdapter LoggingAdapter { get; set; }
        public string Name { get; set; }
        public Action<EtlProcessContext> ContextInitializer { get; set; }
        public List<ObjectPoolSettings> ObjectPoolRegistrations { get; }

        public EtlProcessSettings()
        {
            ObjectPoolRegistrations = new List<ObjectPoolSettings>();
        }

        public EtlProcessSettings WithLoggingAdapter(ILoggingAdapter adapter)
        {
            LoggingAdapter = adapter;
            return this;
        }

        public EtlProcessSettings Named(string name)
        {
            Name = name;
            return this;
        }

        public EtlProcessSettings WithContextInitializer(Action<EtlProcessContext> initializer)
        {
            ContextInitializer = initializer;
            return this;
        }

        public EtlProcessSettings RegisterObjectPool<T>(int initialSize = 5000, bool autoGrow = true)
        {
            ObjectPoolRegistrations.Add(new ObjectPoolSettings(typeof(T), initialSize, autoGrow));
            return this;
        }

        public class ObjectPoolSettings
        {
            public int InitialSize { get; }
            public bool AutoGrow { get; }
            public Type Type { get; }

            public ObjectPoolSettings(Type type, int initialSize, bool autoGrow)
            {
                Type = type;
                AutoGrow = autoGrow;
                InitialSize = initialSize;
            }
        }
    }
}