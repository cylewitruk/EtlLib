using System.IO;
using System.Linq;
using EtlLib.Data;
using EtlLib.Pipeline;
using Renci.SshNet;

namespace EtlLib.Nodes.Ssh
{
    public class SftpFileUploaderNode : AbstractProcessingNode<NodeOutputWithFilePath, SftpFileUploadResult>
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _username;
        private readonly string _remoteDirectory;

        private PasswordAuthenticationMethod _passwordAuthentication;
        private PrivateKeyAuthenticationMethod _privateKeyAuthentication;

        public SftpFileUploaderNode(string host, int port, string username, string remoteDirectory)
        {
            _host = host;
            _port = port;
            _username = username;
            _remoteDirectory = remoteDirectory;
        }

        public SftpFileUploaderNode WithPasswordAuthentication(string username, string password)
        {
            _passwordAuthentication = new PasswordAuthenticationMethod(username, password);
            return this;
        }

        public SftpFileUploaderNode WithPasswordAuthentication(string username, byte[] password)
        {
            _passwordAuthentication = new PasswordAuthenticationMethod(username, password);
            return this;
        }

        public SftpFileUploaderNode WithPrivateKeyAuthentication(string username, params string[] privateKeyFilePath)
        {
            _privateKeyAuthentication =
                new PrivateKeyAuthenticationMethod(username, privateKeyFilePath.Select(x => new PrivateKeyFile(x)).ToArray());
            return this;
        }

        public SftpFileUploaderNode WithPrivateKeyAuthentication(string username, params PrivateKeyFile[] privateKeys)
        {
            _privateKeyAuthentication = new PrivateKeyAuthenticationMethod(username, privateKeys);
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var connectionInfo = new ConnectionInfo(_host, _port, _username);
            
            if (_passwordAuthentication != null)
                connectionInfo.AuthenticationMethods.Add(_passwordAuthentication);

            if (_privateKeyAuthentication != null)
                connectionInfo.AuthenticationMethods.Add(_privateKeyAuthentication);

            using (var client = new SftpClient(connectionInfo))
            {
                client.Connect();

                foreach (var file in Input)
                {
                    var remotePath = Path.Combine(_remoteDirectory, Path.GetFileName(file.FilePath));

                    using (var stream = File.OpenRead(file.FilePath))
                        client.UploadFile(stream, remotePath);

                    Emit(new SftpFileUploadResult(file.FilePath, remotePath));
                }

                client.Disconnect();
            }

            SignalEnd();
        }
    }
}