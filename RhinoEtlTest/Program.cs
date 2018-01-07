using System;
using RhinoEtlTest.Processes;

namespace RhinoEtlTest
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var fromDate = DateTime.UtcNow.AddDays(-1).Date;
            var toDate = DateTime.UtcNow.Date;

            new JournalEtlProcess(fromDate, toDate)
                .Execute();
        }
    }
}
