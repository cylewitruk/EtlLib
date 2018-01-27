using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.Linq;
using AutoFixture.Xunit2;
using EtlLib.Data;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Builders;
using Dapper;
using DapperExtensions;
using DapperExtensions.Mapper;
using DapperExtensions.Sql;
using EtlLib.Pipeline.Operations;
using EtlLib.UnitTests;
using FluentAssertions;
using Xunit;

namespace EtlLib.Nodes.Dapper.IntegrationTests
{
    public class DapperReaderNodeTests
    {
        [Theory]
        [AutoData]
        public void Dapper_reader_can_perform_simple_select_in_etlprocess(TestTable[] tests)
        {
            DapperExtensions.DapperExtensions.SqlDialect = new SqliteDialect();

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");
                foreach (var test in tests)
                    con.Insert(test);

                var context = new EtlPipelineContext();

                var result = EtlProcessBuilder.Create(context)
                    .Input(ctx => new DapperReaderNode<TestTable>(() => con, "select Id, Timestamp, Name from TestTable;"))
                    .CompleteWithResult()
                    .Build()
                    .ExecuteWithResult(context);

                var results = result.Result.ToArray();
                results.Length.Should().Be(tests.Length);
                results.Should().HaveSameCount(tests);
                results.Should().BeEquivalentTo(tests, config => config.Excluding(x => x.IsFrozen));
            }
        }

        [Theory]
        [AutoData]
        public void Dapper_node_can_perform_simple_select_and_emits_items(TestTable[] tests)
        {
            DapperExtensions.DapperExtensions.SqlDialect = new SqliteDialect();

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");
                foreach (var test in tests)
                    con.Insert(test);

                var context = new EtlPipelineContext();

                var emitter = new TestEmitter<TestTable>();
                var node = new DapperReaderNode<TestTable>(() => con, "select Id, Timestamp, Name from TestTable;");
                node.SetEmitter(emitter);
                node.Execute(context);

                emitter.HasSignalledEnd.Should().BeTrue();
                var results = emitter.EmittedItems;
                results.Count.Should().Be(tests.Length);
                results.Should().HaveSameCount(tests);
                results.Should().BeEquivalentTo(tests, config => config.Excluding(x => x.IsFrozen));
            }
        }

        [Fact]
        public void Dapper_node_raises_error_and_signals_end_with_bad_query()
        {
            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();

                var context = new EtlPipelineContext();

                var emitter = new TestEmitter<TestTable>();
                var errorHandler = new TestErrorHandler();
                var node = new DapperReaderNode<TestTable>(() => con, "select Id, Timestamp, Name from TestTable;");
                node.SetEmitter(emitter).SetErrorHandler(errorHandler);
                node.Execute(context);

                errorHandler.Errors.Should().HaveCount(1);
                emitter.HasSignalledEnd.Should().BeTrue();
            }
        }

        public class TestTableMapper : ClassMapper<TestTable>
        {
            public TestTableMapper()
            {
                Table("TestTable");
                Map(x => x.Id).Key(KeyType.Assigned);
                Map(x => x.IsFrozen).Ignore();
                AutoMap();
            }
        }

        public class TestTable : INodeOutput<TestTable>
        {
            public int Id { get; set; }
            public DateTime Timestamp { get; set; }
            public string Name { get; set; }

            public void CopyTo(TestTable obj)
            {
                obj.Id = Id;
                obj.Timestamp = Timestamp;
                obj.Name = Name;
            }

            public void Reset()
            {
                Id = default(int);
                Timestamp = default(DateTime);
                Name = default(string);
            }

            public bool IsFrozen { get; private set; }
            public void Freeze()
            {
                IsFrozen = true;
            }
        }
    }
}