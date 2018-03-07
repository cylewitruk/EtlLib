using EtlLib.Nodes.Redshift.Builders;
using EtlLib.Nodes.Redshift.Builders.Copy;
using FluentAssertions;
using Xunit;

namespace EtlLib.Nodes.Redshift.Tests
{
    public class CopyCommandBuilderTests
    {
        [Fact]
        public void Can_generate_copy_from_s3_using_iam_role_sql()
        {
            var builder = new RedshiftCopyCommandBuilder();
            builder
                .From.S3("hello", s3 => s3
                    .Region("eu-west-1")
                    .UsingObjectPrefix("hello_world")
                )
                .AuthorizedBy.IamRole("iam:role/1234")
                .To("new_table");

            var sql = builder.Build();

            const string expected = 
@"COPY new_table
FROM 's3://hello/hello_world'
IAM_ROLE 'iam:role/1234'
REGION 'eu-west-1'";

            sql.Should().Be(expected);
        }

        [Fact]
        public void Can_generate_copy_from_s3_using_access_key_sql()
        {
            var builder = new RedshiftCopyCommandBuilder();
            builder
                .From.S3("hello", s3 => s3
                    .Region("eu-west-1")
                    .UsingObjectPrefix("hello_world")
                )
                .AuthorizedBy.AccessKey("hello", "world")
                .To("new_table");

            var sql = builder.Build();

            const string expected =
                @"COPY new_table
FROM 's3://hello/hello_world'
ACCESS_KEY_ID 'hello' SECRET_ACCESS_KEY 'world'
REGION 'eu-west-1'";

            sql.Should().Be(expected);
        }

        [Fact]
        public void Can_generate_copy_from_s3_with_csv_format_sql()
        {
            var builder = new RedshiftCopyCommandBuilder();
            builder
                .From.S3("hello", s3 => s3
                    .Region("eu-west-1")
                    .UsingObjectPrefix("hello_world")
                    .FileFormat.Csv(csv => { })
                )
                .AuthorizedBy.IamRole("iam:role/1234")
                .To("new_table");

            var sql = builder.Build();

            const string expected =
                @"COPY new_table
FROM 's3://hello/hello_world'
IAM_ROLE 'iam:role/1234'
REGION 'eu-west-1'
CSV";

            sql.Should().Be(expected);
        }

        [Fact]
        public void Can_generate_copy_from_s3_with_csv_format_and_custom_delimiter_sql()
        {
            var builder = new RedshiftCopyCommandBuilder();
            builder
                .From.S3("hello", s3 => s3
                    .UsingObjectPrefix("hello_world")
                    .FileFormat.Csv(csv => csv.DelimitedBy(";"))
                )
                .AuthorizedBy.IamRole("iam:role/1234")
                .To("new_table");

            var sql = builder.Build();

            const string expected =
                @"COPY new_table
FROM 's3://hello/hello_world'
IAM_ROLE 'iam:role/1234'
CSV DELIMITER AS ';'";

            sql.Should().Be(expected);
        }

        [Fact]
        public void Can_generate_copy_from_s3_with_csv_format_and_encoding_specified()
        {
            var builder = new RedshiftCopyCommandBuilder();
            builder
                .From.S3("hello", s3 => s3
                    .Region("eu-west-1")
                    .UsingObjectPrefix("hello_world")
                    .FileFormat.Csv(csv => csv.EncodedAs.UTF16())
                )
                .AuthorizedBy.IamRole("iam:role/1234")
                .To("new_table");

            var sql = builder.Build();

            const string expected =
                @"COPY new_table
FROM 's3://hello/hello_world'
IAM_ROLE 'iam:role/1234'
REGION 'eu-west-1'
CSV ENCODING AS UTF16";

            sql.Should().Be(expected);
        }
    }
}