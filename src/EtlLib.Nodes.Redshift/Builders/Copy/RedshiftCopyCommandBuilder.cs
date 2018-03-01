using System;
using System.Text;

namespace EtlLib.Nodes.Redshift.Builders.Copy
{
    public enum CopyFromAuthorizedBy
    {
        Undefined,
        IamRole,
        AccessKey
    }

    public enum CopyFromSourceType
    {
        Undefined,
        S3,
        RemoteHost,
        DynamoDB,
        EC2
    }

    public interface IRedshiftCopyCommandBuilder
    {
        IRedshiftCopyCommandBuilder To(string tableName);
        IRedshiftCopyFromBuilder From { get; }
        IRedshiftCopyFromAuthorizedByBuilder AuthorizedBy { get; }
        IRedshiftCopyCommandBuilder WithColumnList(params string[] columns);
        IRedshiftCopyCommandBuilder IgnoreHeaders(int numberOfRows);
    }

    public interface IRedshiftCopyFromAuthorizedByBuilder
    {
        IRedshiftCopyCommandBuilder IamRole(string iamRoleArn);
        IRedshiftCopyCommandBuilder AccessKey(string accessKeyId, string accessKeySecret);
    }

    public interface IRedshiftCopyFromBuilder
    {
        IRedshiftCopyCommandBuilder S3(string bucketName, Action<IRedshiftCopyFromS3Builder> s3);
    }

    public interface IRedshiftCopyFromS3Builder
    {
        IRedshiftCopyFromS3Builder Region(string region);
        IRedshiftCopyFromS3Builder UsingManifestFile(string manifestFile);
        IRedshiftCopyFromS3Builder UsingObjectPrefix(string objectPrefix);
        IRedshiftCopyFromS3FileFormatBuilder FileFormat { get; }
        IRedshiftCopyFromS3CompressedUsingBuilder CompressedUsing { get; }
    }

    public interface IRedshiftCopyFromS3CompressedUsingBuilder
    {
        IRedshiftCopyFromS3Builder Gzip();
        IRedshiftCopyFromS3Builder Bzip2();
        IRedshiftCopyFromS3Builder Lzop();
    }

    public interface IRedshiftCopyFromS3FileFormatBuilder
    {
        IRedshiftCopyFromS3Builder Csv(Action<IRedshiftCopyFromCsvBuilder> csv);
    }

    public interface IRedshiftCopyFromCsvBuilder
    {
        IRedshiftCopyFromCsvBuilder DelimitedBy(string delimiter);
        IRedshiftCopyFromCsvBuilder QuoteAs(string quote);
        IRedshiftCopyFromCsvBuilder EncodingAs(string encoiding);
    }

    public class RedshiftCopyCommandBuilder : IRedshiftBuilder, IRedshiftCopyCommandBuilder, IRedshiftCopyFromAuthorizedByBuilder, IRedshiftCopyFromBuilder
    {
        private string 
            _toTableName, 
            _iamRole,
            _accessKeyId,
            _accessKeySecret;

        private string[] _columnList;
        private RedshiftCopyFromS3Builder _copyFromS3Builder;
        private CopyFromAuthorizedBy _authorizedBy;
        private CopyFromSourceType _copyFromSource;
        private int _ignoreHeaderRowCount = 0;

        public RedshiftCopyCommandBuilder()
        {
            _authorizedBy = CopyFromAuthorizedBy.Undefined;
            _copyFromSource = CopyFromSourceType.Undefined;
        }

        public IRedshiftCopyCommandBuilder To(string tableName)
        {
            _toTableName = tableName;
            return this;
        }

        public IRedshiftCopyFromBuilder From => this;
        public IRedshiftCopyFromAuthorizedByBuilder AuthorizedBy => this;
        public IRedshiftCopyCommandBuilder WithColumnList(params string[] columns)
        {
            _columnList = columns;
            return this;
        }

        public IRedshiftCopyCommandBuilder IamRole(string iamRoleArn)
        {
            _authorizedBy = CopyFromAuthorizedBy.IamRole;
            _iamRole = iamRoleArn;
            return this;
        }

        public IRedshiftCopyCommandBuilder AccessKey(string accessKeyId, string accessKeySecret)
        {
            _authorizedBy = CopyFromAuthorizedBy.AccessKey;
            _accessKeyId = accessKeyId;
            _accessKeySecret = accessKeySecret;
            return this;
        }

        public IRedshiftCopyCommandBuilder S3(string bucketName, Action<IRedshiftCopyFromS3Builder> s3)
        {
            _copyFromSource = CopyFromSourceType.S3;
            _copyFromS3Builder = new RedshiftCopyFromS3Builder(bucketName);
            s3(_copyFromS3Builder);
            return this;
        }

        public IRedshiftCopyCommandBuilder IgnoreHeaders(int numberOfRows = 1)
        {
            _ignoreHeaderRowCount = numberOfRows;
            return this;
        }

        public string Build()
        {
            var sb = new StringBuilder();
            
            sb.AppendLine(BuildCopyStatement());
            sb.AppendLine(BuildFromStatement());
            sb.AppendLine(BuildAuthorizationStatement());

            if (_copyFromSource == CopyFromSourceType.S3)
            {
                if (_copyFromS3Builder.AmazonRegion != null)
                    sb.AppendLine($"REGION '{_copyFromS3Builder.AmazonRegion}'");
                if (_copyFromS3Builder.ObjectKeyType == CopyFromS3SourceObjectType.ManifestFile)
                    sb.AppendLine("MANIFEST");
                if (_copyFromS3Builder.IsUsingCompression)
                {
                    switch (_copyFromS3Builder.CompressionType)
                    {
                        case CopyFromS3CompressedUsing.Bzip2:
                            sb.AppendLine("BZIP2");
                            break;
                        case CopyFromS3CompressedUsing.Gzip:
                            sb.AppendLine("GZIP");
                            break;
                        case CopyFromS3CompressedUsing.Lzop:
                            sb.AppendLine("LZOP");
                            break;
                    }
                }
                    
            }

            sb.AppendLine(BuildFormatStatement());
            sb.AppendLine(BuildDataConversionStatement());

            return sb.ToString().Trim();
        }

        private string BuildDataConversionStatement()
        {
            var statement = new StringBuilder();

            if (_ignoreHeaderRowCount > 0)
                statement.AppendLine($"IGNOREHEADER AS {_ignoreHeaderRowCount}");

            return statement.ToString();
        }

        private string BuildCopyStatement()
        {
            var statement = $"COPY {_toTableName}";
            if (_columnList != null)
                statement += $" ({string.Join(",", _columnList)})";
            return statement;
        }

        private string BuildFromStatement()
        {
            switch (_copyFromSource)
            {
                case CopyFromSourceType.Undefined:
                    throw new RedshiftCommandBuilderException("The copy from source must be specified.");
                case CopyFromSourceType.S3:
                    return $"FROM 's3://{_copyFromS3Builder.BucketName}/{_copyFromS3Builder.ObjectKey}'";
                default:
                    throw new RedshiftCommandBuilderException($"Copy from source type '{_copyFromSource}' is not currently supported.");
            }
        }

        private string BuildAuthorizationStatement()
        {
            switch (_authorizedBy)
            {
                case CopyFromAuthorizedBy.Undefined:
                    throw new RedshiftCommandBuilderException("The authorized by clause must be specified.");
                case CopyFromAuthorizedBy.IamRole:
                    return $"IAM_ROLE '{_iamRole}'";
                case CopyFromAuthorizedBy.AccessKey:
                    return $"ACCESS_KEY_ID '{_accessKeyId}' SECRET_ACCESS_KEY '{_accessKeySecret}'";
                default:
                    throw new RedshiftCommandBuilderException($"Copy from authorization type '{_authorizedBy}' is not currently supported.");
            }
        }

        private string BuildFormatStatement()
        {
            var statement = string.Empty;

            switch (_copyFromS3Builder.FileFormatType)
            {
                case CopyFromS3FileFormat.Csv:
                {
                    statement = "CSV";
                    if(_copyFromS3Builder.FileEncodingSpecified)
                        statement += $" ENCODING AS {_copyFromS3Builder.FileEncoding}";
                    if (_copyFromS3Builder.CustomQuotesSpecified)
                        statement += $" QUOTE AS '{_copyFromS3Builder.CustomQuote}'";
                    if (_copyFromS3Builder.CustomDelimiterSpecified)
                        statement += $" DELIMITER AS '{_copyFromS3Builder.Delimiter}'";
                    break;
                }
            }

            return statement;
        }
    }
}