using System.IO;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvReaderNode : AbstractOutputNode<Row>
    {
        private readonly bool _hasHeader;
        private readonly string _filePath;

        public CsvReaderNode(string filePath, bool hasHeaderRow = true)
        {
            _filePath = filePath;
            _hasHeader = hasHeaderRow;
        }

        public override void OnExecute(EtlPipelineContext context)
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
                    var row = context.ObjectPool.Borrow<Row>();
                    row.Load(reader.Context.HeaderRecord, reader.Context.Record);
                    Emit(row);
                }

                SignalEnd();
            }
        }
    }
}