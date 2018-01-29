using System;
using System.Collections.Generic;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.AmazonS3
{
    public static class EtlPipelineExtensions
    {
        public static IEtlPipeline UploadFilesToAmazonS3(this IEtlPipeline pipeline, Amazon.RegionEndpoint regionEndpoint, string bucketName,
            IEnumerable<string> files, Action<IAmazonS3WriterConfiguration> conf)
        {
            var operation = new AmazonS3WriterEtlOperation(regionEndpoint, bucketName, files);
            conf(operation);
            pipeline.Run(operation);

            return pipeline;
        }
    }
}