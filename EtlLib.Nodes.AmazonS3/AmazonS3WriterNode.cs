using System;
using System.IO;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EtlLib.Data;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3WriterNodeResult : Frozen
    {
        public string ETag { get; }
        public DateTime? Expiration { get; }

        public AmazonS3WriterNodeResult(PutObjectResponse result)
        {
            ETag = result.ETag;
            Expiration = result.Expiration?.ExpiryDate;
        }
    }

    public class AmazonS3WriterNode : AbstractInputOutputNode<IHasFilePath, AmazonS3WriterNodeResult>
    {
        private readonly string _bucketName;
        private AWSCredentials _awsCredentials;
        private readonly Amazon.RegionEndpoint _awsRegionEndpoint;

        public AmazonS3WriterNode(Amazon.RegionEndpoint regionEndpoint, string bucketName)
        {
            _awsCredentials = new AnonymousAWSCredentials();
            _awsRegionEndpoint = regionEndpoint;
            _bucketName = bucketName;
        }

        public AmazonS3WriterNode WithAnonymousCredentials()
        {
            _awsCredentials = new AnonymousAWSCredentials();
            return this;
        }

        public AmazonS3WriterNode WithBasicCredentials(string accessKey, string secretKey)
        {
            _awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            return this;
        }

        public AmazonS3WriterNode WithEnvironmentVariableCredentials()
        {
            _awsCredentials = new EnvironmentVariablesAWSCredentials();
            return this;
        }

        public override void Execute()
        {
            using (var client = new AmazonS3Client(_awsCredentials, _awsRegionEndpoint))
            {
                foreach (var row in Input)
                {
                    var objectKey = Path.GetFileName(row.FilePath);

                    var request = new PutObjectRequest()
                    {

                        BucketName = _bucketName,
                        Key = objectKey,
                        FilePath = row.FilePath
                    };

                    var result = client.PutObjectAsync(request).GetAwaiter().GetResult();

                    Emit(new AmazonS3WriterNodeResult(result));
                }
            }
        }
    }
}
