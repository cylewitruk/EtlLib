using System;

namespace EtlLib.Nodes.Redshift.Builders.Copy
{
    public enum CopyFromS3SourceObjectType
    {
        Undefined,
        ObjectPrefix,
        ManifestFile
    }

    public enum CopyFromS3CompressedUsing
    {
        Undefined,
        Gzip,
        Bzip2,
        Lzop
    }

    public enum CopyFromS3FileFormat
    {
        Undefined,
        Csv,
        FixedWidth,
        Json,
        Avro
    }

    public class RedshiftCopyFromS3Builder : IRedshiftCopyFromS3Builder, IRedshiftCopyFromS3FileFormatBuilder, IRedshiftCopyFromCsvBuilder, IRedshiftCopyFromS3CompressedUsingBuilder
    {
        public string BucketName { get; }
        public string AmazonRegion { get; private set; }
        public string ObjectKey { get; private set; }
        public CopyFromS3SourceObjectType ObjectKeyType { get; private set; }
        public bool IsUsingCompression { get; private set; }
        public CopyFromS3CompressedUsing CompressionType { get; private set; }
        public bool CustomDelimiterSpecified { get; private set; }
        public string Delimiter { get; private set; }
        public bool CustomQuotesSpecified { get; private set; }
        public string CustomQuote { get; private set; }
        public CopyFromS3FileFormat FileFormatType {get; private set;}
        public string FileEncoding { get; private set; }
        public bool FileEncodingSpecified { get; private set; }

        public IRedshiftCopyFromS3FileFormatBuilder FileFormat => this;
        public IRedshiftCopyFromS3CompressedUsingBuilder CompressedUsing => this;

        public RedshiftCopyFromS3Builder(string bucketName)
        {
            BucketName = bucketName;
            ObjectKeyType = CopyFromS3SourceObjectType.Undefined;
            CompressionType = CopyFromS3CompressedUsing.Undefined;
            FileFormatType = CopyFromS3FileFormat.Undefined;
        }

        public IRedshiftCopyFromS3Builder Region(string region)
        {
            AmazonRegion = region;
            return this;
        }

        public IRedshiftCopyFromS3Builder UsingManifestFile(string manifestFile)
        {
            ObjectKey = manifestFile;
            ObjectKeyType = CopyFromS3SourceObjectType.ManifestFile;
            return this;
        }

        public IRedshiftCopyFromS3Builder UsingObjectPrefix(string objectPrefix)
        {
            ObjectKey = objectPrefix;
            ObjectKeyType = CopyFromS3SourceObjectType.ObjectPrefix;
            return this;
        }

        // CSV cannot be used with FIXEDWIDTH, REMOVEQUOTES, or ESCAPE. 
        public IRedshiftCopyFromS3Builder Csv(Action<IRedshiftCopyFromCsvBuilder> csv)
        {
            FileFormatType = CopyFromS3FileFormat.Csv;
            csv(this);
            return this;
        }

        public IRedshiftCopyFromCsvBuilder DelimitedBy(string delimiter)
        {
            CustomDelimiterSpecified = true;
            Delimiter = delimiter;
            return this;
        }

        public IRedshiftCopyFromCsvBuilder QuoteAs(string quote)
        {
            CustomQuotesSpecified = true;
            CustomQuote = quote;
            return this;
        }
        
        public IRedshiftCopyFromCsvBuilder EncodingAs(string encoiding)
        {   
            FileEncodingSpecified = true;
            FileEncoding = encoiding;
            return this;
        }

        public IRedshiftCopyFromS3Builder Gzip()
        {
            IsUsingCompression = true;
            CompressionType = CopyFromS3CompressedUsing.Gzip;
            return this;
        }

        public IRedshiftCopyFromS3Builder Bzip2()
        {
            IsUsingCompression = true;
            CompressionType = CopyFromS3CompressedUsing.Bzip2;
            return this;
        }

        public IRedshiftCopyFromS3Builder Lzop()
        {
            IsUsingCompression = true;
            CompressionType = CopyFromS3CompressedUsing.Lzop;
            return this;
        }
    }
}