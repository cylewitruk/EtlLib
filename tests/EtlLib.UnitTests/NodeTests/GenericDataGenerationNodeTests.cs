using System;
using System.Globalization;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.NodeTests
{
    public class GenericDataGenerationNodeTests
    {
        [Fact]
        public void Can_generate_fixed_number_of_items()
        {
            var node = new GenericDataGenerationNode<Row, object>(5, (ctx, i, gen) =>
            {
                var row = new Row
                {
                    [i.ToString()] = "hello",
                    ["number"] = i.ToString()
                };
                return row;
            });

            var emitter = new TestEmitter<Row>();
            node.SetEmitter(emitter);

            node.Execute(TestHelpers.CreatePipelineContext());

            emitter.EmittedItems.Should().HaveCount(5);
            var items = emitter.EmittedItems;

            for (var i = 1; i <= 5; i++)
            {
                items[i-1].Columns.ContainsKey(i.ToString()).Should().BeTrue();
                items[i-1][i.ToString()].Should().Be("hello");
                items[i-1]["number"].Should().Be(i.ToString());
            }
        }

        [Fact]
        public void Can_generate_items_based_on_predicate()
        {
            var node = new GenericDataGenerationNode<Row, DateTime>(
                gen => gen.State <= DateTime.ParseExact("2018-01-31 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture), 
                (ctx, i, gen) =>
                {
                    if (i == 1)
                        gen.SetState(DateTime.ParseExact("2018-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        
                    var row = new Row
                    {
                        ["date"] = gen.State.ToString("yyyy-MM-dd")
                    };

                    gen.SetState(gen.State.AddDays(1));
                    return row;
                });

            var emitter = new TestEmitter<Row>();
            node.SetEmitter(emitter);

            node.Execute(TestHelpers.CreatePipelineContext());

            emitter.EmittedItems.Should().HaveCount(31);
            var items = emitter.EmittedItems;

            var assertDate = DateTime.ParseExact("2018-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            for (var i = 0; i < 31; i++)
            {
                if (i == 0)
                    items[i]["date"].Should().Be("2018-01-01");

                items[i].Columns.ContainsKey("date").Should().BeTrue();
                items[i]["date"].Should().Be(assertDate.ToString("yyyy-MM-dd"));
                assertDate = assertDate.AddDays(1);
            }
        }
    }
}