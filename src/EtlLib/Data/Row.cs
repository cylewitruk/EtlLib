using System;
using System.Collections;
using System.Collections.Generic;

namespace EtlLib.Data
{
    public class Row : INodeOutput<Row>, IEnumerable<KeyValuePair<string, object>>
    {
        private readonly Dictionary<string, object> _columns;
        private volatile bool _isFrozen;

        public IReadOnlyDictionary<string, object> Columns => _columns;
        public bool IsFrozen => _isFrozen;
        public int ColumnCount => _columns.Count;

        public object this[string key]
        {
            get => _columns[key];
            set
            {
                if (IsFrozen)
                    throw new InvalidOperationException("Cannot modify Row because the Row has been frozen.");

                _columns[key] = value;
            }
        }

        public Row()
        {
            _isFrozen = false;
            _columns = new Dictionary<string, object>();
        }

        public void Freeze() => _isFrozen = true;

        public bool HasColumn(string columnName)
        {
            return _columns.ContainsKey(columnName);
        }

        public static Row FromArray(string[] columns, object[] values)
        {
            if (columns.Length != values.Length)
                throw new ArgumentException("Length of columns and values must be the same.");

            var row = new Row();
            for (var i = 0; i < columns.Length; i++)
            {
                row[columns[i]] = values[i];
            }
            return row;
        }

        public void Merge(Row row, bool overwriteExisting = false)
        {
            foreach (var column in row)
            {
                if (HasColumn(column.Key) && !overwriteExisting)
                    continue;

                this[column.Key] = column.Value;
            }
        }

        public Row Copy()
        {
            var row = new Row();
            foreach (var item in _columns)
                row[item.Key] = item.Value;
            return row;
        }

        public void Load(string[] columns, object[] values)
        {
            if (columns.Length != values.Length)
                throw new ArgumentException("Length of columns and values must be the same.");
            
            for (var i = 0; i < columns.Length; i++)
            {
                this[columns[i]] = values[i];
            }
        }

        public void CopyTo(Row row)
        {
            foreach (var item in _columns)
                row[item.Key] = item.Value;
        }

        public void Reset()
        {
            _columns.Clear();
            _isFrozen = false;
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
           return  _columns.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    //TODO: Implement retry logic?  Thinking mostly in the case where web requests are involved which can fail
    public class RowProperties
    {
        public int MaxRetries { get; set; }
        public int RetryCount { get; set; }
    }
}