using System.Data.SQLite;
using AutoFixture.Xunit2;
using Dapper;
using DapperExtensions.Sql;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;

namespace EtlLib.Nodes.Dapper.IntegrationTests
{
    public class DapperExecuteScalarOperationTests
    {
        [Fact]
        public void Can_execute_simple_count_statements()
        {
            DapperExtensions.DapperExtensions.SqlDialect = new SqliteDialect();

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");

                var operation = new DapperExecuteScalarOperation<int>(() => con, "select count(*) from TestTable;");

                var context = new EtlPipelineContext();

                var result = operation.ExecuteWithResult(context);
                result.Should().NotBeNull();
                result.Result.Should().Be(0);
            }

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");

                var operation = new DapperExecuteScalarOperation<int>(() => con, "select count(*) from TestTable;");

                var context = new EtlPipelineContext();

                con.Execute("insert into TestTable values (1, '2018-01-26 00:00:00', 'Hello world!');");
                con.Execute("insert into TestTable values (2, '2018-01-27 00:00:00', 'Hello world 2!');");

                var result = operation.ExecuteWithResult(context);
                result.Should().NotBeNull();
                result.Result.Should().Be(2);
            }
        }
    }
}