using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Logging;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvReaderNode : AbstractOutputNode<Row>
    {
        private readonly bool _hasHeader;
        private string _filePath;
        private ILogger _log;

        public CsvReaderNode(bool hasHeader = true, string filePath = null, string stateKey = null)
        {
            _filePath = filePath;
            if (!string.IsNullOrWhiteSpace(stateKey))
                WithFilePathFromStateKey(stateKey);

            _hasHeader = hasHeader;
        }

        public CsvReaderNode WithSpecifiedFilePath(string filePath)
        {
            _filePath = filePath;
            return this;
        }

        public CsvReaderNode WithFilePathFromStateKey(string key)
        {
            _filePath = (string) Context.State[key];
            return this;
        }

        public override void Execute()
        {
            _log = Context.LoggingAdapter.CreateLogger("EtlLib.Nodes.CsvReaderNode");

            using (var file = File.OpenText(_filePath))
            using (var reader = new CsvReader(file))
            {
                reader.Configuration.BadDataFound = null;
                reader.Read();
                if (_hasHeader)
                    reader.ReadHeader();

                while (reader.Read())
                {
                    Emit(Row.FromArray(reader.Context.HeaderRecord, reader.Context.Record));
                }

                Emitter.SignalEnd();
            }
        }
    }
}