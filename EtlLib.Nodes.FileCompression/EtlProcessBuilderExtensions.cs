using System;
using EtlLib.Data;
using EtlLib.Pipeline.Builders;

namespace EtlLib.Nodes.FileCompression
{
    public static class EtlProcessBuilderExtensions
    {
        public static IOutputNodeBuilderContext<NodeOutputWithFilePath> GZipFiles(
            this IOutputNodeBuilderContext<NodeOutputWithFilePath> builder, Action<GZipFileCompressionNode> cfg)
        {
            var node = new GZipFileCompressionNode();
            cfg(node);
            return builder.Continue(ctx => node);
        }

        public static IOutputNodeBuilderContext<NodeOutputWithFilePath> BZip2Files(
            this IOutputNodeBuilderContext<NodeOutputWithFilePath> builder, Action<BZip2FileCompressionNode> cfg)
        {
            var node = new BZip2FileCompressionNode();
            cfg(node);
            return builder.Continue(ctx => node);
        }
    }
}