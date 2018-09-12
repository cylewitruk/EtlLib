using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using CsvHelper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvWriterNode : AbstractProcessingNode<Row, NodeOutputWithFilePath>
    {
        private readonly string _filePath;

        private bool _includeHeader;
        private bool _quoteAllFields;
        private int _writtenRowCount;
        private Encoding _encoding;
        private CultureInfo _culture;
        private string _nullAs;
  
        public CsvWriterNode(string filePath)
        {
            _filePath = filePath;
            _includeHeader = true;
            _encoding = Encoding.UTF8;
            _culture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
            _culture.DateTimeFormat.FullDateTimePattern = "yyyy-MM-dd HH:mm:ss.fff";
            _culture.DateTimeFormat.ShortDatePattern = "yyyy-MM-dd";
            _culture.DateTimeFormat.ShortTimePattern = "HH:mm:ss";
        }

        public CsvWriterNode IncludeHeader(bool includeHeaders = true)
        {
            _includeHeader = includeHeaders;
            return this;
        }

        public CsvWriterNode WithEncoding(Encoding encoding)
        {
            _encoding = encoding;
            return this;
        }
       
        public CsvWriterNode QuoteAllFields(bool quoteAllFields = true)
        {
            _quoteAllFields = quoteAllFields;
            return this;
        }


        public CsvWriterNode WithCulture(CultureInfo cultureInfo)
        {
            _culture = cultureInfo;
            return this;
        }

        public CsvWriterNode WithNullAs(string nullAs)
        {
            _nullAs = nullAs;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var log = context.GetLogger("EtlLib.Nodes.CsvWriterNode");
            var first = true;
            var columns = new List<string>();

            using (var file = File.OpenWrite(_filePath))
            using (var sw = new StreamWriter(file, _encoding))
            using (var writer = new CsvWriter(sw))
            {
                writer.Configuration.QuoteAllFields = _quoteAllFields;
                writer.Configuration.CultureInfo = _culture;

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
                        writer.WriteField(column.Value ?? _nullAs);
                    }

                    writer.NextRecord();
                    context.ObjectPool.Return(row);
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