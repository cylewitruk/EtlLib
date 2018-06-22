using System.Linq;
using EtlLib.Data;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlProcessTests
{
    public class EtlProcessTests
    {
        [Fact]
        public void Simple_etl_process_scenario_test()
        {
            var process = EtlProcessBuilder.Create()
                .GenerateInput<Row, int>(gen => gen.State < 5, (ctx, i, gen) =>
                {
                    gen.SetState(gen.State + 1);
                    return new Row { ["number"] = gen.State };
                })
                .Transform((ctx, row) =>
                {
                    var newRow = ctx.ObjectPool.Borrow<Row>();
                    row.CopyTo(newRow);
                    newRow["transformed"] = true;
                    ctx.ObjectPool.Return(row);
                    return newRow;
                })
                .CompleteWithResult()
                .Build();

            var context = new EtlPipelineContext();

            var result = process.Execute(context);

            result.Should().BeAssignableTo<IEnumerableEtlOperationResult<Row>>();
            var enumerableResult = result as IEnumerableEtlOperationResult<Row>;
            var results = enumerableResult.Result.ToList();
            results.Count.Should().Be(5);
            for (var i = 1; i <= 5; i++)
            {
                results[i-1]["number"].Should().Be(i);
                results[i-1]["transformed"].Should().Be(true);
            }
        }
    }
}