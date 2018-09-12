using System;
using System.Collections.Generic;
using System.IO;
using Amazon.Runtime;
using Amazon.S3;
using EtlLib.Logging;
using Amazon.S3.Transfer;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Nodes.AmazonS3
{
    public class AmazonS3WriterEtlOperation : AbstractEtlOperationWithEnumerableResult<AmazonS3WriterResult>, IAmazonS3WriterConfiguration
    {
        private readonly string _bucketName;
        private AWSCredentials _awsCredentials;
        private readonly Amazon.RegionEndpoint _awsRegionEndpoint;
        private readonly IEnumerable<string> _files;
        private S3StorageClass _storageClass;
        private DateTime startTime;
        private int progress;
        
        public AmazonS3WriterEtlOperation(Amazon.RegionEndpoint regionEndpoint, string bucketName, IEnumerable<string> files)
        {
            _awsCredentials = new AnonymousAWSCredentials();
            _storageClass = S3StorageClass.Standard;
            _awsRegionEndpoint = regionEndpoint;
            _bucketName = bucketName;
            _files = files;
        }

        public AmazonS3WriterEtlOperation WithAnonymousCredentials()
        {
            _awsCredentials = new AnonymousAWSCredentials();
            return this;
        }

        public AmazonS3WriterEtlOperation WithBasicCredentials(string accessKey, string secretKey)
        {
            _awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            return this;
        }

        public AmazonS3WriterEtlOperation WithEnvironmentVariableCredentials()
        {
            _awsCredentials = new EnvironmentVariablesAWSCredentials();
            return this;
        }

        public AmazonS3WriterEtlOperation WithStorageClass(S3StorageClass storageClass)
        {
            _storageClass = storageClass;
            return this;
        }

        IAmazonS3WriterConfiguration IAmazonS3WriterConfiguration.WithAnonymousCredentials() =>
            WithAnonymousCredentials();

        IAmazonS3WriterConfiguration IAmazonS3WriterConfiguration.WithBasicCredentials(string accessKey,
            string secretKey) => WithBasicCredentials(accessKey, secretKey);

        IAmazonS3WriterConfiguration IAmazonS3WriterConfiguration.WithEnvironmentVariableCredentials() =>
            WithEnvironmentVariableCredentials();

        IAmazonS3WriterConfiguration IAmazonS3WriterConfiguration.WithStorageClass(S3StorageClass storageClass) =>
            WithStorageClass(storageClass);

        public override IEnumerableEtlOperationResult<AmazonS3WriterResult> ExecuteWithResult(EtlPipelineContext context)
        {
            var logger = context.GetLogger(GetType().FullName);

            if ((_awsCredentials == null || _awsCredentials is AnonymousAWSCredentials) && context.Config.ContainsKey(Constants.S3AccessKeyId))
                _awsCredentials = new BasicAWSCredentials(context.Config[Constants.S3AccessKeyId], context.Config[Constants.S3SecretAccessKey]);

            var results = new List<AmazonS3WriterResult>();
            using (var client = new TransferUtility(new AmazonS3Client(_awsCredentials, _awsRegionEndpoint)))
            {
                foreach (var file in _files)
                {
                    var objectKey = Path.GetFileName(file);

                    var request = new TransferUtilityUploadRequest()
                    {
                        BucketName = _bucketName,
                        Key = objectKey,
                        FilePath = file,
                        StorageClass = _storageClass
                    };

                    startTime = DateTime.Now;
                    progress = 0;

                    request.UploadProgressEvent += (sender, e) =>
                    {
                        if (progress == e.PercentDone || !((DateTime.Now - startTime).TotalSeconds > 0))
                            return;

                        var bs = e.TransferredBytes / (DateTime.Now - startTime).TotalSeconds;
                        var kbs = bs / 1024;

                        logger.Info($"Upploading {e.FilePath}, progress {e.PercentDone}%, {kbs:0.00} KB/s");
                        progress = e.PercentDone;
                    };

                    client.UploadAsync(request).GetAwaiter().OnCompleted(() =>
                    {
                        results.Add(new AmazonS3WriterResult(objectKey, null));
                    });
                }
            }

            return new EnumerableEtlOperationResult<AmazonS3WriterResult>(true, results);
        }
    }
}