using System;
using System.IO;
using System.Threading.Tasks;
using EtlLib.Data;
using EtlLib.Pipeline;
using ICSharpCode.SharpZipLib.BZip2;

namespace EtlLib.Nodes.FileCompression
{
    public class BZip2FileCompressionNode : AbstractInputOutputNode<NodeOutputWithFilePath, NodeOutputWithFilePath>
    {
        private int _compressionLevel, _degreeOfParallelism;
        private string _fileSuffix;

        public BZip2FileCompressionNode()
        {
            _compressionLevel = 5;
            _degreeOfParallelism = 1;
            _fileSuffix = ".bzip2";
        }

        /// <summary>
        /// Block size acts as the compression level (1 to 9) with 1 being the lowest compression and 9 being the highest.
        /// </summary>
        /// <param name="compressionLevel">The block size (compression level), 1 to 9.</param>
        public BZip2FileCompressionNode CompressionLevel(int compressionLevel)
        {
            if (compressionLevel < 1 || compressionLevel > 9)
                throw new ArgumentException("BZip2 compression level must be between 1 and 9.", nameof(compressionLevel));

            _compressionLevel = compressionLevel;
            return this;
        }

        public BZip2FileCompressionNode Parallelize(int degreeOfParallelism)
        {
            _degreeOfParallelism = degreeOfParallelism;
            return this;
        }

        public BZip2FileCompressionNode FileSuffix(string suffix)
        {
            _fileSuffix = suffix;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            Parallel.ForEach(Input, new ParallelOptions {MaxDegreeOfParallelism = _degreeOfParallelism}, item =>
            {
                var outFileName = item.FilePath + _fileSuffix;
                BZip2.Compress(File.OpenRead(item.FilePath), File.OpenWrite(outFileName), true, _compressionLevel);
                Emit(new NodeOutputWithFilePath(outFileName));
            });

            SignalEnd();
        }
    }
}
