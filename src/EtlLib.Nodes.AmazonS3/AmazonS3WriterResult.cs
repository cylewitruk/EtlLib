using System;
using Amazon.S3.Model;
using EtlLib.Data;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3WriterResult : Frozen<AmazonS3WriterResult>, INodeOutput<AmazonS3WriterResult>
    {
        public string ObjectKey { get; private set; }
        public string ETag { get; private set; }
        public DateTime? Expiration { get; private set; }

        public AmazonS3WriterResult() { }

        public AmazonS3WriterResult(string objectKey, PutObjectResponse result)
        {
            ObjectKey = objectKey;
            ETag = result.ETag;
            Expiration = result.Expiration?.ExpiryDate;
        }

        public AmazonS3WriterResult(string objectKey)
        {
            ObjectKey = objectKey;
        }

        public void Reset()
        {
            ObjectKey = null;
            ETag = null;
            Expiration = null;
        }

        public void CopyTo(AmazonS3WriterResult obj)
        {
            obj.ObjectKey = ObjectKey;
            obj.ETag = ETag;
            obj.Expiration = Expiration;
        }
    }
}