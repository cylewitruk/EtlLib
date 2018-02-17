using EtlLib.Data;

namespace EtlLib.Nodes.Ssh
{
    public class SftpFileUploadResult : NodeOutputWithFilePath, INodeOutput<SftpFileUploadResult>
    {
        public string RemotePath { get; private set; }

        public SftpFileUploadResult(string filePath, string remotePath)
        : base(filePath)
        {
            RemotePath = remotePath;
        }

        public SftpFileUploadResult() { }

        public void CopyTo(SftpFileUploadResult obj)
        {
            obj.RemotePath = RemotePath;
            base.CopyTo(obj);
        }

        public override void Reset()
        {
            RemotePath = null;
            base.Reset();
        }
    }
}