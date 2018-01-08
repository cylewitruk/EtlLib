using System;
using System.Runtime.InteropServices.ComTypes;
using EtlLib.Data;
using EtlLib.Logging.NLog;
using EtlLib.Nodes.CsvFiles;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline.Builders;

namespace EtlLib.ConsoleTest
{
    internal class Program
    {
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
                .Input(ctx => new CsvReaderNode(filePath: @"C:\Users\Cyle\Downloads\baseballdatabank-2017.1\baseballdatabank-2017.1\core\Batting.csv"))
                .GenerateRowNumbers("_id")
                .Filter(row => !string.IsNullOrWhiteSpace((string)row["RBI"]))
                .Continue(ctx => new GenericFilterNode<Row>(row => row.GetAs<int>("RBI") > 10))
                .Filter(row => row.GetAs<int>("HR") > 1)
                //.BlockingExecute((ctx, builder) => builder.)
                .Transform((ctx, row) =>
                {
                    var newRow = ctx.ObjectPool.Borrow<Row>();
                    //var newRow = row.Copy();
                    row.CopyTo(newRow);
                    newRow["is_transformed"] = true;
                    ctx.ObjectPool.Return(row);
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

    public class MapTest : INodeOutput<MapTest>
    {
        public long Id { get; private set; }
        public bool IsFrozen { get; private set; }
        public void Freeze()
        {
            IsFrozen = true;
        }

        public void CopyTo(MapTest obj)
        {
            obj.Id = Id;
        }

        public void Reset()
        {
            Id = 0;
            IsFrozen = false;
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
