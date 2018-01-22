using System;
using Amazon;
using EtlLib.Data;
using EtlLib.Logging.NLog;
using EtlLib.Nodes.AmazonS3;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Nodes.FileCompression;
using EtlLib.Nodes.Redshift;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Builders;

namespace EtlLib.ConsoleTest
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var loggingAdapter = new NLogLoggingAdapter();
            EtlLibConfig.LoggingAdapter = loggingAdapter;
            EtlLibConfig.EnableDebug = true;

            var builder = EtlProcessBuilder.Create()
                .Input(ctx => new CsvReaderNode(@"C:\Users\Cyle\Downloads\LoanStats3a.csv\LoanStats3a.csv"))
                .GenerateRowNumbers("_id")
                .Classify("income_segment", cat =>
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
                .BZip2Files()
                .CompleteWithResult(ctx => new AmazonS3WriterNode(***REMOVED***, "***REMOVED***")
                    .WithBasicCredentials("***REMOVED***", "***REMOVED***")
                );

            //builder.PrintGraph();

            var process = builder.Build();

            var pipelineResult = EtlPipeline.Create(cfg =>
                {
                    cfg
                        .Named("Test ETL Process")
                        //.RegisterObjectPool<Row>(50000)
                        .WithContextInitializer(ctx =>
                        {
                            ctx.Config["s3_bucket_name"] = "***REMOVED***";
                            ctx.Config["s3_access_key"] = "***REMOVED***";
                            ctx.Config["s3_secret_access_key"] = "***REMOVED***";
                            ctx.Config["outfile"] = @"C:\Users\Cyle\Desktop\cyle_d_date.csv";
                        });
                })
                .RunWithResult(process)
                .ForEachResult((context, i, arg3) =>
                {
                    context.State["s3_filename"] = arg3.ObjectKey;
                    Console.WriteLine($"Setting 's3_filename' to '{arg3.ObjectKey}'");
                })
                /*.Run(ctx => new GenerateDateDimensionEtlProcess(
                    ctx.Config["s3_bucket_name"], ctx.Config["s3_access_key"], ctx.Config["s3_secret_access_key"], ctx.Config["outfile"]))
                .Run(ctx => new ExecuteRedshiftBatchOperation("Name", "connectionString", red =>
                {
                    /*red.Execute(cmd => cmd.Create
                        .Table("staging_customers", tbl => tbl
                            .IfNotExists()
                            .Temporary()
                            .NoBackup()
                            .WithColumns(cols =>
                            {
                                cols.Add("my_id", t => t.AsInt8())
                                    .Identity(1, 1)
                                    .Unique()
                                    .DistributionKey()
                                    .Nullable();

                                cols.Add("amount", t => t.AsDecimal(8, 2));
                            })
                            .SortKey.Interleaved("my_id", "amount")
                            .PrimaryKey("my_id", "amount")
                            .UniqueKey("my_id", "amount")
                        ));

                    red.Execute(cmd => cmd.Copy
                        .To("stage.doesntexist")
                        .From.S3("bucketName", s3 => s3
                            .UsingObjectPrefix("cyle_d_date.csv.bzip2")
                            .FileFormat.Csv(csv => csv
                                .DelimitedBy(",")
                                .QuoteAs("%"))
                            .CompressedUsing.Bzip2()
                        )
                        .AuthorizedBy.AccessKey(ctx.Config["s3_access_key"], ctx.Config["s3_secret_access_key"])
                    );
                }))*/
                .Execute();



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
