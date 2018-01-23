using EtlLib.Data;
using EtlLib.Pipeline;
using EtlLib.Support;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.EtlProcess
{
    public class InputOutputAdapterTests
    {
        [Fact]
        public void InputOutputAdapter_works()
        {
            var outputNode = new TestOutputNode((ctx, e) =>
            {
                for (var i = 1; i <= 5; i++)
                {
                    e.Emit(new Row {["number"] = i});
                }

                e.SignalEnd();
            });

            var inputNode = new TestInputNode();
            var nodeStatistics = new NodeStatistics();

            var ioAdapter = new InputOutputAdapter<Row>(outputNode);
            ioAdapter.SetNodeStatisticsCollector(nodeStatistics);

            //outputNode.SetEmitter(ioAdapter);
            ioAdapter.AttachConsumer(inputNode);
            //inputNode.SetInput(ioAdapter.GetConsumingEnumerable(inputNode));
            //nodeStatistics.RegisterNode(inputNode);
            //nodeStatistics.RegisterNode(outputNode);

            var context = new EtlPipelineContext();

            outputNode.Execute(context);
            inputNode.Execute(context);

            inputNode.ReceivedItems.Count.Should().Be(5);
            for (var i = 1; i <= 5; i++)
            {
                inputNode.ReceivedItems[i - 1]["number"].Should().Be(i);
            }

            nodeStatistics.TotalReads.Should().Be(5);
            nodeStatistics.TotalWrites.Should().Be(5);
            nodeStatistics.TotalErrors.Should().Be(0);
        }
    }
}