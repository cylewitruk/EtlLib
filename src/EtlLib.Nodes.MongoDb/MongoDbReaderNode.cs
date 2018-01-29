using System;
using EtlLib.Data;
using EtlLib.Pipeline;
using MongoDB.Driver;

namespace EtlLib.Nodes.MongoDb
{
    public class MongoDbReaderNode<T> : AbstractOutputNode<T>
        where T : class, INodeOutput<T>, new()
    {
        private readonly string _connectionString, _databaseName, _collectionName;
        private FilterDefinition<T> _filter;
        private SortDefinition<T> _sort;

        public MongoDbReaderNode(string connectionString, string databaseName, string collectionName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
            _collectionName = collectionName;
        }

        public MongoDbReaderNode<T> Query(Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> filter)
        {
            _filter = filter(Builders<T>.Filter);
            return this;
        }

        public MongoDbReaderNode<T> Sort(Func<SortDefinitionBuilder<T>, SortDefinition<T>> sort)
        {
            _sort = sort(Builders<T>.Sort);
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var client = MongoStatic.GetClient(_connectionString);
            var database = client.GetDatabase(_databaseName);
            var collection = database.GetCollection<T>(_collectionName);

            using (var cursor = collection.FindSync(_filter, new FindOptions<T> {Sort = _sort}))
            {
                while (cursor.MoveNext())
                {
                    foreach(var document in cursor.Current)
                        Emit(document);
                }
            }

            SignalEnd();
        }
    }
}
