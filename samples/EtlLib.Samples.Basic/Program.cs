using System;
using EtlLib.Logging.NLog;
using EtlLib.Pipeline;

namespace EtlLib.Samples.Basic
{
    class Program
    {
        static void Main(string[] args)
        {
            EtlLibConfig.LoggingAdapter = new NLogLoggingAdapter();

            var context = new EtlPipelineContext();
        }
    }
}
