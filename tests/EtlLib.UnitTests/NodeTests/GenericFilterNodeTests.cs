using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace EtlLib.UnitTests.NodeTests
{
    public class GenericFilterNodeTests : TestBase
    {
        public GenericFilterNodeTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_filter_on_predicate()
        {
            var input = new List<Row>
            {
                new Row {["number"] = 1},
                new Row {["number"] = 2},
                new Row {["number"] = 3},
                new Row {["number"] = 4},
                new Row {["number"] = 5},
                new Row {["number"] = 6}
            };

            var node = new GenericFilterNode<Row>(row => row.GetAs<int>("number") % 2 == 0);
            var emitter = new TestEmitter<Row>();

            node.SetInput(input);
            node.SetEmitter(emitter);

            node.Execute(TestHelpers.CreatePipelineContext());

            emitter.EmittedItems.Should().HaveCount(3);
            emitter.EmittedItems[0]["number"].Should().Be(2);
            emitter.EmittedItems[1]["number"].Should().Be(4);
            emitter.EmittedItems[2]["number"].Should().Be(6);
        }

        
    }
}