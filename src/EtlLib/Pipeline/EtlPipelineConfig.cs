using System.Collections.Generic;

namespace EtlLib.Pipeline
{
    public interface IEtlPipelineConfig
    {
        string this[string key] { get; }
        bool ContainsKey(string key);
    }

    public class EtlPipelineConfig : IEtlPipelineConfig
    {
        private readonly Dictionary<string, string> _dict;

        public string this[string key]
        {
            get => _dict[key];
            set => _dict[key] = value;
        }

        public EtlPipelineConfig()
        {
            _dict = new Dictionary<string, string>();
        }

        public EtlPipelineConfig Set(string key, string value)
        {
            _dict[key] = value;
            return this;
        }

        public bool ContainsKey(string key)
        {
            return _dict.ContainsKey(key);
        }
    }
}