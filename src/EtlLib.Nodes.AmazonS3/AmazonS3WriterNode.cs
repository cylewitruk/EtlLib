using System.IO;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3WriterNode : AbstractInputOutputNode<NodeOutputWithFilePath, AmazonS3WriterResult>
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

        public override void OnExecute(EtlPipelineContext context)
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

                    Emit(new AmazonS3WriterResult(objectKey, result));
                }
            }

            SignalEnd();
        }
    }
}
