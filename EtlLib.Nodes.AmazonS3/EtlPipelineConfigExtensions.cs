using EtlLib.Pipeline;

namespace EtlLib.Nodes.AmazonS3
{
    public static class EtlPipelineConfigExtensions
    {
        public static EtlPipelineConfig SetAmazonS3BasicCredentials(this EtlPipelineConfig config, string accessKeyId,
            string secretAccessKey)
        {
            config[Constants.S3AccessKeyId] = accessKeyId;
            config[Constants.S3SecretAccessKey] = secretAccessKey;
            return config;
        }
    }

    internal static class Constants
    {
        public const string S3AccessKeyId = "${s3_access_key_id}";
        public const string S3SecretAccessKey = "${s3_secret_access_key}";
    }
}