using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace EtlLib.UnitTests.NodeTests
{
    public class GenericClassificationNodeTests : TestBase
    {
        public GenericClassificationNodeTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_classify_data_with_multiple_whens_and_a_default()
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

            var node = new GenericClassificationNode<Row, string, object>(row => row["class"])
                .When(x => x.GetAs<int>("number") % 3 == 0, "MOD_3")
                .When(x => x.GetAs<int>("number") % 2 == 0, "MOD_2")
                .Default("NOT_MOD_2_OR_3");

            var emitter = new TestEmitter<Row>();

            node.SetInput(input);
            node.SetEmitter(emitter);

            node.Execute(TestHelpers.CreatePipelineContext());

            emitter.EmittedItems.Should().HaveCount(6);
            emitter.EmittedItems[0]["class"].Should().Be("NOT_MOD_2_OR_3");
            emitter.EmittedItems[1]["class"].Should().Be("MOD_2");
            emitter.EmittedItems[2]["class"].Should().Be("MOD_3");
            emitter.EmittedItems[3]["class"].Should().Be("MOD_2");
            emitter.EmittedItems[4]["class"].Should().Be("NOT_MOD_2_OR_3");
            emitter.EmittedItems[5]["class"].Should().Be("MOD_3");
        }

        [Fact]
        public void Can_create_when_memberexpression_used()
        {
            // Passing variable results in a MemberExpression
            var columnName = "class"; 
            new GenericClassificationNode<Row, string, object>(row => row[columnName])
                .When(x => x.GetAs<int>("number") % 3 == 0, "MOD_3")
                .When(x => x.GetAs<int>("number") % 2 == 0, "MOD_2")
                .Default("NOT_MOD_2_OR_3");
        }

        [Fact]
        public void Can_create_when_constantexpression_used()
        {
            // Passing a constant string results in a ConstantExpression
            new GenericClassificationNode<Row, string, object>(row => row["class"])
                .When(x => x.GetAs<int>("number") % 3 == 0, "MOD_3")
                .When(x => x.GetAs<int>("number") % 2 == 0, "MOD_2")
                .Default("NOT_MOD_2_OR_3");
        }
    }
}