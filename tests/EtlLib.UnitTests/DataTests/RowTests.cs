using System.Linq;
using EtlLib.Data;
using FluentAssertions;
using Xunit;

namespace EtlLib.UnitTests.DataTests
{
    public class RowTests
    {
        [Fact]
        public void Row_merge_with_no_overriding()
        {
            var row1 = new Row {["1"] = 2, ["2"] = 3};
            var row2 = new Row {["3"] = 4, ["2"] = "x"};

            row1.Merge(row2);

            row1.Count().Should().Be(4);
            row1.ColumnCount.Should().Be(4);
            row1["1"].Should().Be(2);
            row1["2"].Should().Be(3);
            row1["2_2"].Should().Be("x");
            row1["3"].Should().Be(4);
        }

        [Fact]
        public void Row_merge_with_overriding()
        {
            var row1 = new Row { ["1"] = 2, ["2"] = 3 };
            var row2 = new Row { ["3"] = 4, ["2"] = "x" };

            row1.Merge(row2, true);

            row1.Count().Should().Be(3);
            row1.ColumnCount.Should().Be(3);
            row1["1"].Should().Be(2);
            row1["2"].Should().Be("x");
            row1["3"].Should().Be(4);
        }
    }
}