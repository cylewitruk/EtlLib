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

            //EtlPipeline
            //    .Builder
            //        .Input(ctx => new CsvReaderNode())
            //        .Transform(ctx => new Transformation1())
            //        .LeftJoin(Etl.Input(ctx => new AdoNetReaderNode(input1ConnectionString)))

            var loggingAdapter = new NLogLoggingAdapter();

            var builder = EtlProcessBuilder
                .Create(cfg =>
                {
                    cfg
                        .WithLoggingAdapter(loggingAdapter)
                        .Named("Test Process");
                })
                .Input(ctx => new CsvReaderNode(filePath: @"C:\Users\Cyle\Downloads\baseballdatabank-2017.1\baseballdatabank-2017.1\core\Batting.csv"))
                .Filter(row => !string.IsNullOrWhiteSpace((string)row["RBI"]))
                .Continue(ctx => new GenericFilterNode<Row>(row => row.GetAs<int>("RBI") > 10))
                .Filter(row => row.GetAs<int>("HR") > 1)
                //.Map(row => new MapTest { Id = (long)row["id"] })
                .Continue(ctx => new GenericTransformationNode<Row>((state, row) =>
                {
                    if (!state.ContainsKey("_id"))
                        state["_id"] = 0;
                    else
                        state["_id"] = (int)state["_id"] + 1;

                    var newRow = row.Copy();
                    newRow["_id"] = state["_id"];
                    return newRow;
                }))
                .Transform(row =>
                {
                    var newRow = row.Copy();
                    newRow["is_transformed"] = true;
                    return newRow;
                })
                //.Branch(
                //    (ctx, b1) => b1
                //        .Filter(row => row.GetAs<bool>("is_branch1"))
                //        .Filter(row => row.GetAs<int>("id") % 2 == 0),
                //    (ctx, b2) => b2
                //        .Filter(row => row.GetAs<bool>("is_branch2"))
                //)
                //.MergeResults()
                .Complete(ctx => new CsvWriterNode(filePath: @"C:\Users\Cyle\Downloads\baseballdatabank-2017.1\baseballdatabank-2017.1\core\Batting_TRANSFORMED.csv"));

            builder.PrintGraph();

            var process = builder.Build();

            process.Execute();

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
}
