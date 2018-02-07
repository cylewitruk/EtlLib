using System.Collections.Generic;
using Amazon.Runtime;
using Amazon.S3;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3DeleteEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly string _bucketName;
        private AWSCredentials _awsCredentials;
        private readonly Amazon.RegionEndpoint _awsRegionEndpoint;
        private readonly IEnumerable<string> _objectKeys;

        public AmazonS3DeleteEtlOperation(Amazon.RegionEndpoint regionEndpoint, string bucketName, IEnumerable<string> objectKeys)
        {
            _awsRegionEndpoint = regionEndpoint;
            _bucketName = bucketName;
            _objectKeys = objectKeys;
        }

        public AmazonS3DeleteEtlOperation WithAnonymousCredentials()
        {
            _awsCredentials = new AnonymousAWSCredentials();
            return this;
        }

        public AmazonS3DeleteEtlOperation WithBasicCredentials(string accessKey, string secretKey)
        {
            _awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            return this;
        }

        public AmazonS3DeleteEtlOperation WithEnvironmentVariableCredentials()
        {
            _awsCredentials = new EnvironmentVariablesAWSCredentials();
            return this;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            if ((_awsCredentials == null || _awsCredentials is AnonymousAWSCredentials) &&
                context.Config.ContainsKey(Constants.S3AccessKeyId))
                _awsCredentials = new BasicAWSCredentials(context.Config[Constants.S3AccessKeyId],
                    context.Config[Constants.S3SecretAccessKey]);
            
            using (var client = new AmazonS3Client(_awsCredentials, _awsRegionEndpoint))
            {
                foreach (var objectKey in _objectKeys)
                {
                    var response = client.DeleteObjectAsync(_bucketName, objectKey).GetAwaiter().GetResult();
                }
            }

            return new EtlOperationResult(true);
        }
    }
}