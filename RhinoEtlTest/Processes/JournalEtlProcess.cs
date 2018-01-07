using System;
using Rhino.Etl.Core;
using RhinoEtlTest.Steps;

namespace RhinoEtlTest.Processes
{
    public class JournalEtlProcess : EtlProcess
    {
        private readonly DateTime _fromDate, _toDate;

        public JournalEtlProcess(DateTime fromDate, DateTime toDate)
        {
            _fromDate = fromDate;
            _toDate = toDate;
        }

        protected override void Initialize()
        {
            Register(new ReadJournals("PaynovaDataWarehouse", _fromDate, _toDate));
            Register(new CreateCompositeKey());
            Register(new WriteJournalsToFile($"journals_{_fromDate:yyyyMMdd}_{_toDate:yyyyMMdd}.csv"));
        }
    }
}
