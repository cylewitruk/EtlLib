using System;
using System.Collections.Generic;
using FileHelpers;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Files;
using Rhino.Etl.Core.Operations;

namespace RhinoEtlTest.Steps
{
    public class WriteJournalsToFile : AbstractOperation
    {
        private readonly string _fileName;

        public WriteJournalsToFile(string fileName)
        {
            _fileName = fileName;
        }

        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            var engine = FluentFile.For<JournalCsvFile>();
            engine.HeaderText =
                "composite_key,source,booked_at_utc,booked_at_swe,sequence_number,merchant_id,creditor_id,debit_amount,credit_amount,account_number,journal_id";
            using (var file = engine.To(_fileName))
            {
                foreach (var row in rows)
                {
                    var record = new JournalCsvFile
                    {
                        CompositeKey = (string)row["composite_key"],
                        Source = (string) row["source"],
                        BookedAtUtc = (DateTime) row["booked_at_utc"],
                        BookedAtSwe = ((DateTime) row["booked_at_utc"]).ToLocalTime(),
                        SequenceNumber = (long) row["sequence_number"],
                        MerchantId = (long) (row["merchant_id"] ?? -1L),
                        CreditorId = (long) row["creditor_id"],
                        DebitAmount = (decimal) row["debit_amount"],
                        CreditAmount = (decimal) row["credit_amount"],
                        AccountNumber = (int) row["account_number"],
                        JournalId = (int) row["journal_id"]
                    };

                    file.Write(record);
                }
                yield break;
            }
        }
    }

    [DelimitedRecord(",")]
    public class JournalCsvFile
    {
        public string CompositeKey;
        public string Source;

        [FieldConverter(ConverterKind.Date, "yyyy-MM-dd HH:mm:ss")] public DateTime BookedAtUtc;

        [FieldConverter(ConverterKind.Date, "yyyy-MM-dd HH:mm:ss")] public DateTime BookedAtSwe;

        public long SequenceNumber;
        public long MerchantId;
        public long CreditorId;
        public decimal DebitAmount;
        public decimal CreditAmount;
        public int AccountNumber;
        public int JournalId;
    }
}