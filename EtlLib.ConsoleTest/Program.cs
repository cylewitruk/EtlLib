using System;
using EtlLib.Data;
using EtlLib.Logging.NLog;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline.Builders;

namespace EtlLib.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var loggingAdapter = new NLogLoggingAdapter();

            var builder = EtlProcessBuilder
                .Create(cfg =>
                {
                    cfg
                        .WithLoggingAdapter(loggingAdapter)
                        .Named("Test Process")
                        .WithContextInitializer(ctx => ctx.StateDict["hello"] = "world!");
                })
                .Input(ctx => new CsvReaderNode(filePath: @"C:\Users\Cyle\Downloads\baseballdatabank-2017.1\baseballdatabank-2017.1\core\Batting.csv"))
                .GenerateRowNumbers("_id")
                .Filter(row => !string.IsNullOrWhiteSpace((string)row["RBI"]))
                .Continue(ctx => new GenericFilterNode<Row>(row => row.GetAs<int>("RBI") > 10))
                .Filter(row => row.GetAs<int>("HR") > 1)
                .Transform(row =>
                {
                    var newRow = row.Copy();
                    newRow["is_transformed"] = true;
                    return newRow;
                })
                .Complete(ctx => new CsvWriterNode(filePath: @"C:\Users\Cyle\Downloads\baseballdatabank-2017.1\baseballdatabank-2017.1\core\Batting_TRANSFORMED.csv"));

            builder.PrintGraph();

            var process = builder.Build();

            process.Execute();

            Console.WriteLine("\nPress enter to exit...\n");
            Console.ReadLine();
        }
    }

    public class MapTest : IFreezable
    {
        public long Id { get; set; }
        public bool IsFrozen { get; }
        public void Freeze()
        {
            throw new System.NotImplementedException();
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
