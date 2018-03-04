namespace EtlLib.Nodes.Dapper
{
    public enum InsertTransactionStrategy
    {
        TransactionPerInsert,
        AllInOneTransaction,
        Batch
    }
}