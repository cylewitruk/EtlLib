using System.IO;
using CsvHelper;
using EtlLib.Data;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvReaderNode : AbstractOutputNode<Row>
    {
        private readonly bool _hasHeader;
        private string _filePath;

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
            _filePath = (string) Context.StateDict[key];
            return this;
        }

        public override void OnExecute()
        {
            using (var file = File.OpenText(_filePath))
            using (var reader = new CsvReader(file))
            {
                reader.Configuration.BadDataFound = null;
                reader.Read();
                if (_hasHeader)
                    reader.ReadHeader();

                while (reader.Read())
                {
                    var row = Context.ObjectPool.Borrow<Row>();
                    row.Load(reader.Context.HeaderRecord, reader.Context.Record);
                    Emit(row);
                    //Emit(Row.FromArray(reader.Context.HeaderRecord, reader.Context.Record));
                }

                Emitter.SignalEnd();
            }
        }
    }
}