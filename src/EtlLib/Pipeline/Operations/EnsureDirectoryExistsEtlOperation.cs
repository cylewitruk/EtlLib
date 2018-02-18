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
            var log = context.GetLogger(GetType().FullName);

            log.Debug($"Ensuring existance of directory '{_path}'.");
            Directory.CreateDirectory(_path);
            return new EtlOperationResult(true);
        }
    }
}