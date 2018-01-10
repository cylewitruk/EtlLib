using System;
using Amazon;
using EtlLib.Data;
using EtlLib.Logging.NLog;
using EtlLib.Nodes.AmazonS3;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline.Builders;

namespace EtlLib.ConsoleTest
{
    internal class Program
    {
        // S3 ***REMOVED*** (AccessKey: ***REMOVED***, SecretKey: ***REMOVED***)
        private static void Main(string[] args)
        {
            var loggingAdapter = new NLogLoggingAdapter();

            var builder = EtlProcessBuilder
                .Create(cfg =>
                {
                    cfg
                        .WithLoggingAdapter(loggingAdapter)
                        .Named("Test Process")
                        .WithContextInitializer(ctx => ctx.StateDict["hello"] = "world!")
                        .RegisterObjectPool<Row>(100000);
                })
                .Input(ctx => new CsvReaderNode(filePath: @"C:\Users\Cyle\Downloads\LoanStats3a.csv\LoanStats3a.csv"))
                .GenerateRowNumbers("_id")
                .Categorize("income_segment", cat =>
                {
                    decimal Income(Row row) => row.GetAs<decimal>("annual_inc");

                    cat
                        .When(x => string.IsNullOrWhiteSpace(x.GetAs<string>("annual_inc")), "UNKNOWN")
                        .When(x => Income(x) < 10000L, "0-9999")
                        .When(x => Income(x) < 20000L, "10000-19999")
                        .When(x => Income(x) < 30000L, "20000-29999")
                        .Default("30000+");
                })
                .Filter(row => row.GetAs<string>("grade") == "A")
                .Transform((ctx, row) =>
                {
                    var newRow = ctx.ObjectPool.Borrow<Row>();
                    row.CopyTo(newRow);
                    newRow["is_transformed"] = true;
                    ctx.ObjectPool.Return(row);
                    return newRow;
                })
                .Continue(ctx => new CsvWriterNode(filePath: @"C:\Users\Cyle\Downloads\LoanStats3a.csv\LoanStats3a_TRANSFORMED.csv"))
                .Complete(ctx => new AmazonS3WriterNode(***REMOVED***, "***REMOVED***")
                    .WithBasicCredentials("***REMOVED***", "***REMOVED***")
                );

            builder.PrintGraph();

            var process = builder.Build();

            process.Execute();

            Console.WriteLine("\nPress enter to exit...\n");
            Console.ReadLine();
        }
    }

    //EtlPipeline
    //    .Builder
    //        .Input(ctx => new CsvReaderNode())
    //        .Transform(ctx => new Transformation1())
    //        .LeftJoin(Etl.Input(ctx => new AdoNetReaderNode(input1ConnectionString)))

    //.Map(row => new MapTest { Id = (long)row["id"] })

    //.Branch(
    //    (ctx, b1) => b1
    //        .Filter(row => row.GetAs<bool>("is_branch1"))
    //        .Filter(row => row.GetAs<int>("id") % 2 == 0),
    //    (ctx, b2) => b2
    //        .Filter(row => row.GetAs<bool>("is_branch2"))
    //)
    //.MergeResults()
}
