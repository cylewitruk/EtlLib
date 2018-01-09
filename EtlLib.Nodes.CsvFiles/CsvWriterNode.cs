using System.Collections.Generic;
using System.IO;
using CsvHelper;
using EtlLib.Data;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvWriterNode : AbstractInputOutputNode<Row, NodeOutputWithFilePath>
    {
        private string _filePath;
        private bool _includeHeader;
        private int _writtenRowCount;

        public CsvWriterNode(string filePath = null, string stateKey = null)
        {
            _filePath = filePath;
            _includeHeader = true;

            if (!string.IsNullOrWhiteSpace(stateKey))
                WithFilePathFromStateKey(stateKey);
        }

        public CsvWriterNode WithSpecifiedFilePath(string filePath)
        {
            _filePath = filePath;
            return this;
        }

        public CsvWriterNode WithFilePathFromStateKey(string key)
        {
            _filePath = (string)Context.StateDict[key];
            return this;
        }

        public CsvWriterNode IncludeHeader(bool includeHeaders = true)
        {
            _includeHeader = includeHeaders;
            return this;
        }

        public override void OnExecute()
        {
            var log = Context.LoggingAdapter.CreateLogger("EtlLib.Nodes.CsvWriterNode");
            var first = true;
            var columns = new List<string>();

            using (var file = File.OpenWrite(_filePath))
            using (var sw = new StreamWriter(file))
            using (var writer = new CsvWriter(sw))
            {
                foreach (var row in Input)
                {
                    if (first && _includeHeader)
                    {
                        foreach (var column in row)
                        {
                            writer.WriteField(column.Key);
                            columns.Add(column.Key);
                        }
                        writer.NextRecord();
                        first = false;
                    }

                    foreach (var column in row)
                    {
                        writer.WriteField(column.Value);
                    }
                    writer.NextRecord();

                    Context.ObjectPool.Return(row);
                    _writtenRowCount++;
                }

                writer.Flush();
            }

            if (_includeHeader)
                _writtenRowCount++;

            log.Debug($"{this} wrote {_writtenRowCount} rows to '{_filePath}");

            Emit(new CsvWriterNodeResult(_filePath, _writtenRowCount, columns.ToArray(), _includeHeader));
            SignalEnd();
        }
    }
}