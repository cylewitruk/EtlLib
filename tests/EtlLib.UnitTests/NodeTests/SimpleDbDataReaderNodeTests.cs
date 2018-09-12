using System;
using System.Data.SQLite;
using AutoFixture.Xunit2;
using Dapper;
using DapperExtensions;
using DapperExtensions.Mapper;
using DapperExtensions.Sql;
using EtlLib.Data;
using EtlLib.Nodes.Impl;
using EtlLib.Pipeline;
using FluentAssertions;
using Xunit;
using Moq;

namespace EtlLib.UnitTests.NodeTests
{
    public class SimpleDbDataReaderNodeTests
    {
        [Theory]
        [AutoData]
        public void SimpleDbDataReaderNode_can_perform_simple_select_and_emits_items(TestTable[] tests)
        {
            DapperExtensions.DapperExtensions.SqlDialect = new SqliteDialect();

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");
                foreach (var test in tests)
                    con.Insert(test);
                
                // Mock calls to CreateNamedDbConnection, to make it return the same
                // in memmory connenction every time         
                var mock = new Mock<EtlPipelineContext>();
                mock.Setup(ctx => ctx.CreateNamedDbConnection("DB.Test")).Returns(con);
                var context = mock.Object;     

                // register the connection name
                context.DbConnections
                    .For<SQLiteConnection>(reg => reg
                        .Register("DB.Test", "foo"));
                
                var emitter = new TestEmitter<Row>();
                var node = new SimpleDbDataReaderNode("DB.Test", "select Id, Timestamp, Name from TestTable;");
                node.SetEmitter(emitter);
                node.Execute(context);

                emitter.HasSignalledEnd.Should().BeTrue();

                var results = emitter.EmittedItems;
                results.Count.Should().Be(tests.Length);
                results.Should().HaveSameCount(tests);
                for (var i = 0; i < tests.Length; i++)
                {
                    results[i]["Id"].Should().Be(tests[i].Id);
                    results[i]["Timestamp"].Should().Be(tests[i].Timestamp);
                    results[i]["Name"].Should().Be(tests[i].Name);
                }   
            }
        }

        [Fact]
        public void SimpleDbDataReaderNode_Handles_zero_results_gracefully()
        {
            DapperExtensions.DapperExtensions.SqlDialect = new SqliteDialect();

            using (var con = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;"))
            {
                con.Open();
                con.Execute("create table TestTable(Id int, Timestamp datetime, Name varchar(50));");
                
                // Mock calls to CreateNamedDbConnection, to make it return the same
                // in memmory connenction every time        
                var mock = new Mock<EtlPipelineContext>();
                mock.Setup(ctx => ctx.CreateNamedDbConnection("DB.Test")).Returns(con);
                var context = mock.Object;     

                // register the connection name
                context.DbConnections
                    .For<SQLiteConnection>(reg => reg
                        .Register("DB.Test", "foo"));


                var emitter = new TestEmitter<Row>();
                var node = new SimpleDbDataReaderNode("DB.Test", "select Id, Timestamp, Name from TestTable;");
                node.SetEmitter(emitter);
                node.Execute(context);

                emitter.HasSignalledEnd.Should().BeTrue();

                var results = emitter.EmittedItems;
                results.Count.Should().Be(0);
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