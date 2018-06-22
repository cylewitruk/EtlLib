using System;
using System.Collections.Generic;
using System.Data;
using DapperExtensions;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Dapper
{
    public class DapperWriterNode<TOut> : AbstractSinkNode<TOut> 
        where TOut : class, INodeOutput<TOut>, new()
    {
        private IsolationLevel _isolationLevel;
        private int? _timeoutInSeconds;
        private readonly string _connectionName;
        private Func<IDbConnection> _createConnection;
        private int? _batchSize;
        private InsertTransactionStrategy _transactionStrategy;

        protected DapperWriterNode()
        {
            _isolationLevel = IsolationLevel.ReadCommitted;
        }

        public DapperWriterNode(Func<IDbConnection> createConnectionFn)
            : this()
        {
            _createConnection = createConnectionFn;
        }

        public DapperWriterNode(string connectionName)
            : this()
        {
            _connectionName = connectionName;
        }

        public DapperWriterNode<TOut> WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public DapperWriterNode<TOut> WithTimeout(TimeSpan timeout)
        {
            _timeoutInSeconds = (int)timeout.TotalSeconds;
            return this;
        }

        public DapperWriterNode<TOut> WithTransactionPerInsertStrategy()
        {
            _transactionStrategy = InsertTransactionStrategy.TransactionPerInsert;
            return this;
        }

        public DapperWriterNode<TOut> WithAllInsertsInOneTransactionStrategy()
        {
            _transactionStrategy = InsertTransactionStrategy.AllInOneTransaction;
            return this;
        }

        public DapperWriterNode<TOut> WithBatchInsertTransactionStrategy(int batchSize)
        {
            _transactionStrategy = InsertTransactionStrategy.Batch;
            _batchSize = batchSize;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            if (_createConnection == null)
            {
                _createConnection = () => context.DbConnectionFactory.CreateNamedConnection(_connectionName);
            }

            using (var con = _createConnection())
            {
                switch (_transactionStrategy)
                {
                    case InsertTransactionStrategy.AllInOneTransaction:
                        using (var trx = con.BeginTransaction(_isolationLevel))
                        {
                            con.Insert(Input, trx, _timeoutInSeconds);
                            trx.Commit();
                        }

                        break;
                    case InsertTransactionStrategy.TransactionPerInsert:
                        foreach (var item in Input)
                        {
                            using (var trx = con.BeginTransaction(_isolationLevel))
                            {
                                con.Insert(item, trx, _timeoutInSeconds);
                                trx.Commit();
                            }
                        }

                        break;
                    case InsertTransactionStrategy.Batch:
                        var buffer = new List<TOut>();
                        foreach (var item in Input)
                        {
                            buffer.Add(item);
                            if (buffer.Count != _batchSize) continue;

                            using (var trx = con.BeginTransaction(_isolationLevel))
                            {
                                con.Insert(buffer, trx, _timeoutInSeconds);
                                trx.Commit();
                            }

                            buffer.Clear();
                        }

                        if (buffer.Count > 0)
                        {
                            using (var trx = con.BeginTransaction(_isolationLevel))
                            {
                                con.Insert(buffer, trx, _timeoutInSeconds);
                                trx.Commit();
                            }
                        }

                        break;
                }
            }
        }
    }
}