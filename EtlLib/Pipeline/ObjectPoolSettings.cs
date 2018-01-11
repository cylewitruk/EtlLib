using System;

namespace EtlLib.Pipeline
{
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