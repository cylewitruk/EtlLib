using System;
using System.IO;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EtlLib.Data;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3WriterNodeResult : Frozen<AmazonS3WriterNodeResult>, INodeOutput<AmazonS3WriterNodeResult>
    {
        public string ObjectKey { get; private set; }
        public string ETag { get; private set; }
        public DateTime? Expiration { get; private set; }

        public AmazonS3WriterNodeResult() { }

        public AmazonS3WriterNodeResult(string objectKey, PutObjectResponse result)
        {
            ObjectKey = objectKey;
            ETag = result.ETag;
            Expiration = result.Expiration?.ExpiryDate;
        }

        public void Reset()
        {
            ObjectKey = null;
            ETag = null;
            Expiration = null;
        }

        public void CopyTo(AmazonS3WriterNodeResult obj)
        {
            obj.ObjectKey = ObjectKey;
            obj.ETag = ETag;
            obj.Expiration = Expiration;
        }
    }

    public class AmazonS3WriterNode : AbstractInputOutputNode<NodeOutputWithFilePath, AmazonS3WriterNodeResult>
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

        public override void OnExecute()
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

                    Emit(new AmazonS3WriterNodeResult(objectKey, result));
                }
            }
        }
    }
}
