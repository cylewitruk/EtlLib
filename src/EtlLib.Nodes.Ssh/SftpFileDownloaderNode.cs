using System.IO;
using System.Linq;
using EtlLib.Data;
using EtlLib.Pipeline;
using Renci.SshNet;

namespace EtlLib.Nodes.Ssh
{
    public class SftpFileDownloaderNode : AbstractSourceNode<NodeOutputWithFilePath>
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _username;
        private readonly string _remoteDirectory;
        private readonly string _filenamePrefix;

        private string _localDirectory;
        private PasswordAuthenticationMethod _passwordAuthentication;
        private PrivateKeyAuthenticationMethod _privateKeyAuthentication;

        public SftpFileDownloaderNode(string host, int port, string username, string remoteDirectory, string filenamePrefix)
        {
            _host = host;
            _port = port;
            _username = username;
            _remoteDirectory = remoteDirectory;
            _filenamePrefix = filenamePrefix;
        }

        public SftpFileDownloaderNode ToLocalDirectory(string localDirectory)
        {
            _localDirectory = localDirectory;
            return this;
        }

        public SftpFileDownloaderNode WithPasswordAuthentication(string username, string password)
        {
            _passwordAuthentication = new PasswordAuthenticationMethod(username, password);
            return this;
        }

        public SftpFileDownloaderNode WithPasswordAuthentication(string username, byte[] password)
        {
            _passwordAuthentication = new PasswordAuthenticationMethod(username, password);
            return this;
        }

        public SftpFileDownloaderNode WithPrivateKeyAuthentication(string username, params string[] privateKeyFilePath)
        {
            _privateKeyAuthentication =
                new PrivateKeyAuthenticationMethod(username, privateKeyFilePath.Select(x => new PrivateKeyFile(x)).ToArray());
            return this;
        }

        public SftpFileDownloaderNode WithPrivateKeyAuthentication(string username, params PrivateKeyFile[] privateKeys)
        {
            _privateKeyAuthentication = new PrivateKeyAuthenticationMethod(username, privateKeys);
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var log = context.GetLogger(GetType().FullName);
            var connectionInfo = new ConnectionInfo(_host, _port, _username);

            if (_passwordAuthentication != null)
                connectionInfo.AuthenticationMethods.Add(_passwordAuthentication);

            if (_privateKeyAuthentication != null)
                connectionInfo.AuthenticationMethods.Add(_privateKeyAuthentication);

            var localDirectory = _localDirectory ?? Path.GetTempPath();
            if (_localDirectory == null)
                log.Warn("No explicit download path was specified, using current user's temp directory.");

            log.Debug("Opening connection to SFTP server.");

            using (var client = new SftpClient(connectionInfo))
            {
                client.Connect();

                log.Debug($"Listing contents of remote directory '{_remoteDirectory}' matching filename prefix '{_filenamePrefix}'.");
                var remoteFiles = client.ListDirectory(_remoteDirectory).Where(x => x.Name.StartsWith(_filenamePrefix));
                foreach (var remoteFile in remoteFiles)
                {
                    var localFile = Path.Combine(localDirectory, remoteFile.Name);
                    log.Debug($"Downloading remote file '{remoteFile.FullName}' to '{localFile}'.");

                    using (var file = File.OpenWrite(localFile))
                    {
                        client.DownloadFile(remoteFile.FullName, file);
                    }

                    Emit(new NodeOutputWithFilePath(localFile));
                }

                client.Disconnect();
            }

            SignalEnd();
        }
    }
}