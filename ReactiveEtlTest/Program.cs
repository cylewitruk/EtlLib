using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ReactiveETL;

namespace ReactiveEtlTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var fromDate = DateTime.UtcNow.AddDays(-1).Date;
            var toDate = DateTime.UtcNow.Date;
        }
    }
}
