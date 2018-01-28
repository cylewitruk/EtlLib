using System.IO;

namespace EtlLib.Pipeline.Operations
{
    public class EnsureDirectoryExistsEtlOperation : AbstractEtlOperationWithNoResult
    {
        private readonly string _path;

        public EnsureDirectoryExistsEtlOperation(string path)
        {
            _path = Path.GetDirectoryName(path);
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            Directory.CreateDirectory(_path);
            return new EtlOperationResult(true);
        }
    }
}