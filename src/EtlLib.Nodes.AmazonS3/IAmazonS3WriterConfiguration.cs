using Amazon.S3;

namespace EtlLib.Nodes.AmazonS3
{
    public interface IAmazonS3WriterConfiguration
    {
        IAmazonS3WriterConfiguration WithAnonymousCredentials();
        IAmazonS3WriterConfiguration WithBasicCredentials(string accessKey, string secretKey);
        IAmazonS3WriterConfiguration WithEnvironmentVariableCredentials();
        IAmazonS3WriterConfiguration WithStorageClass(S3StorageClass storageClass);
    }
}