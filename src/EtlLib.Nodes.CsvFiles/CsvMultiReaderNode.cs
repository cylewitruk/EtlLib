using System.IO;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvMultiReaderNode : AbstractProcessingNode<NodeOutputWithFilePath, Row>
    {
        private bool _hasHeader;

        public CsvMultiReaderNode()
        {
            _hasHeader = true;
        }

        public CsvMultiReaderNode HasHeader(bool hasHeader = true)
        {
            _hasHeader = hasHeader;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var input in Input)
            {
                using (var file = File.OpenText(input.FilePath))
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
                }
            }

            SignalEnd();
        }
    }
}