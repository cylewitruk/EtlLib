using System.Collections.Concurrent;
using MongoDB.Driver;

namespace EtlLib.Nodes.MongoDb
{
    public static class MongoStatic
    {
        private static readonly object _lock;
        private static readonly ConcurrentDictionary<string, MongoClient> _clientMap;

        static MongoStatic()
        {
            _lock = new object();
            _clientMap = new ConcurrentDictionary<string, MongoClient>();
        }
        
        public static MongoClient GetClient(string connectionString)
        {
            lock (_lock)
            {
                if (_clientMap.TryGetValue(connectionString, out var client))
                    return client;

                client = new MongoClient(connectionString);
                _clientMap[connectionString] = client;

                return client;
            }
        }
    }
}