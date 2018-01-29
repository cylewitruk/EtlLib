using EtlLib.Data;

namespace EtlLib.Nodes.CsvFiles
{
    public class CsvWriterNodeResult : NodeOutputWithFilePath
    {
        public int RowCount { get; private set; }
        public string[] Columns { get; private set; }
        public bool HasHeaderRow { get; private set; }

        public CsvWriterNodeResult() { }

        public CsvWriterNodeResult(string filePath, int rowCount, string[] columns, bool hasHeaderRow)
            : base(filePath)
        {
            RowCount = rowCount;
            Columns = columns;
            HasHeaderRow = hasHeaderRow;
        }

        public override void Reset()
        {
            RowCount = 0;
            Columns = null;
            HasHeaderRow = false;

            base.Reset();
        }

        public void CopyTo(CsvWriterNodeResult obj)
        {
            obj.RowCount = RowCount;
            obj.Columns = Columns;
            obj.HasHeaderRow = HasHeaderRow;

            base.CopyTo(obj);
        }
    }
}