using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EtlLib.Nodes.Redshift.Builders.Create
{
    public interface IRedshiftCreateTableCommandBuilder
    {
        IRedshiftCreateTableSortKeyBuilder SortKey { get; }

        IRedshiftCreateTableCommandBuilder IfNotExists();
        IRedshiftCreateTableCommandBuilder Temporary();
        IRedshiftCreateTableCommandBuilder NoBackup();
        IRedshiftCreateTableCommandBuilder Like(string otherTable, bool includeDefaults = true);
        IRedshiftCreateTableCommandBuilder WithColumns(Action<IRedshiftColumnListBuilder> cols);
        IRedshiftCreateTableCommandBuilder Distribution(RedshiftTableDistributionStyle distStyle);
        IRedshiftCreateTableCommandBuilder PrimaryKey(params string[] columnNames);
        IRedshiftCreateTableCommandBuilder UniqueKey(params string[] columnNames);
    }

    public interface IRedshiftCreateTableSortKeyBuilder
    {
        IRedshiftCreateTableCommandBuilder Interleaved(params string[] columnNames);
        IRedshiftCreateTableCommandBuilder Compound(params string[] columnNames);
    }

    public enum RedshiftTableDistributionStyle
    {
        Undefined,
        All,
        Even,
        Key
    }

    public enum RedshiftCreateTableMode
    {
        Undefined,
        Like,
        Columns
    }

    public enum RedshiftSortKeyType
    {
        Undefined,
        Compound,
        Interleaved
    }

    public class RedshiftCreateTableCommandBuilder : IRedshiftBuilder, IRedshiftCreateTableCommandBuilder, IRedshiftCreateTableSortKeyBuilder
    {
        public string Name { get; }

        public RedshiftCreateTableMode CreateTableMode { get; private set; }
        public bool IsIfNotExists { get; private set; }
        public bool IsTemporary { get; private set; }
        public bool IsNoBackup { get; private set; }
        public string LikeOtherTableName { get; private set; }
        public bool LikeOtherTableIncludeDefaults { get; private set; }
        public List<RedshiftColumnBuilder> Columns { get; private set; }
        public RedshiftTableDistributionStyle DistributionStyle { get; private set; }
        public IRedshiftCreateTableSortKeyBuilder SortKey => this;
        public RedshiftSortKeyType SortKeyType { get; private set; }
        public string[] SortKeyColumns { get; private set; }
        public bool HasPrimaryKeyConstraint { get; private set; }
        public string[] PrimaryKeyColumns { get; private set; }
        public bool HasUniqueKeyConstraint { get; private set; }
        public string[] UniqueKeyConstraintColumns { get; private set; }

        public RedshiftCreateTableCommandBuilder(string name)
        {
            Name = name;
        }

        public IRedshiftCreateTableCommandBuilder IfNotExists()
        {
            IsIfNotExists = true;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder Temporary()
        {
            IsTemporary = true;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder NoBackup()
        {
            IsNoBackup = true;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder Like(string otherTable, bool includeDefaults = true)
        {
            if (CreateTableMode != RedshiftCreateTableMode.Undefined)
                throw new RedshiftCommandBuilderException($"You have already specified that this table '{Name}' should be created using a list of column definitions. These two creation modes are not compatible.");

            CreateTableMode = RedshiftCreateTableMode.Like;
            LikeOtherTableName = otherTable;
            LikeOtherTableIncludeDefaults = includeDefaults;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder WithColumns(Action<IRedshiftColumnListBuilder> cols)
        {
            if (CreateTableMode != RedshiftCreateTableMode.Undefined)
                throw new RedshiftCommandBuilderException($"You have already specified that this table '{Name}' should be created LIKE '{LikeOtherTableName}'. These two creation modes are not compatible.");
            
            CreateTableMode = RedshiftCreateTableMode.Columns;
            var listBuilder = new RedshiftColumnListBuilder();
            cols(listBuilder);
            Columns = listBuilder.ColumnBuilders;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder Distribution(RedshiftTableDistributionStyle distStyle)
        {
            DistributionStyle = distStyle;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder PrimaryKey(params string[] columnNames)
        {
            var pkColumn = Columns?.SingleOrDefault(x => x.IsPrimaryKey);
            if (pkColumn != null)
                throw new RedshiftCommandBuilderException($"Column '{pkColumn.Name}' is already marked as primary key. A table may only have one primary key.");

            HasPrimaryKeyConstraint = true;
            PrimaryKeyColumns = columnNames;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder UniqueKey(params string[] columnNames)
        {
            //var ukColumn = Columns?.SingleOrDefault(x => x.IsUnique);
            //if (ukColumn != null)
            //    throw new RedshiftCommandBuilderException($"Column '{ukColumn.Name}' is already marked as unique. A table may only have one unique constraint.");

            HasUniqueKeyConstraint = true;
            UniqueKeyConstraintColumns = columnNames;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder Interleaved(params string[] columnNames)
        {
            if (columnNames.Length > 8)
                throw new RedshiftCommandBuilderException("A maximum of 8 columns are allowed to be specified for interleaved sort keys.");

            var sortKeyColumn = Columns?.SingleOrDefault(x => x.IsSortKey);
            if (sortKeyColumn != null)
                throw new RedshiftCommandBuilderException($"Column '{sortKeyColumn.Name}' is already marked as a sort key. You may not combine column- and table-level sort keys.");

            SortKeyType = RedshiftSortKeyType.Interleaved;
            SortKeyColumns = columnNames;
            return this;
        }

        public IRedshiftCreateTableCommandBuilder Compound(params string[] columnNames)
        {
            if (columnNames.Length > 400)
                throw new RedshiftCommandBuilderException("A maximum of 400 columns are allowed to be specified for compound sort keys.");

            var sortKeyColumn = Columns?.SingleOrDefault(x => x.IsSortKey);
            if (sortKeyColumn != null)
                throw new RedshiftCommandBuilderException($"Column '{sortKeyColumn.Name}' is already marked as a sort key. You may not combine column- and table-level sort keys.");

            SortKeyType = RedshiftSortKeyType.Compound;
            SortKeyColumns = columnNames;
            return this;
        }

        public string Build()
        {
            var sb = new StringBuilder();

            sb.AppendFormat("CREATE {0}TABLE {1}{2} \n(", 
                IsTemporary ? "TEMPORARY " : string.Empty,
                IsIfNotExists ? "IF NOT EXISTS " : string.Empty,
                Name);

            if (CreateTableMode == RedshiftCreateTableMode.Like)
            {
                sb.AppendFormat("\nLIKE {0} {1} DEFAULTS\n",
                    LikeOtherTableName,
                    LikeOtherTableIncludeDefaults ? "INCLUDING" : "EXCLUDING");
            }
            else if (CreateTableMode == RedshiftCreateTableMode.Columns)
            {
                for (var i = 0; i < Columns.Count; i++)
                {
                    sb.AppendFormat("\n    {0}{1}", 
                        Columns[i].Build(), 
                        i != Columns.Count - 1 || HasPrimaryKeyConstraint || HasUniqueKeyConstraint ? "," : "");
                }
                sb.Append("\n");
            }

            if (HasPrimaryKeyConstraint)
                sb.AppendFormat("    PRIMARY KEY ({0}){1}\n", string.Join(",", PrimaryKeyColumns), HasUniqueKeyConstraint ? "," : "");
            if (HasUniqueKeyConstraint)
                sb.AppendLine($"    UNIQUE ({string.Join(",", UniqueKeyConstraintColumns)})");

            sb.AppendLine(")");

            sb.AppendFormat("BACKUP {0}\n", IsNoBackup ? "NO" : "YES");

            return sb.ToString();
        }
    }

    public interface IRedshiftColumnListBuilder
    {
        IRedshiftColumnBuilder Add(string name, Action<IRedshiftColumnDataTypeDefinitionBuilder> t);
    }

    public class RedshiftColumnListBuilder : IRedshiftColumnListBuilder
    {
        public List<RedshiftColumnBuilder> ColumnBuilders { get; }

        public RedshiftColumnListBuilder()
        {
            ColumnBuilders = new List<RedshiftColumnBuilder>();
        }

        public IRedshiftColumnBuilder Add(string name, Action<IRedshiftColumnDataTypeDefinitionBuilder> t)
        {
            var columnBuilder = new RedshiftColumnBuilder(name);
            t(columnBuilder.DataTypeDefinition);

            var sortKeyColumn = ColumnBuilders.SingleOrDefault(x => x.IsSortKey);
            if (sortKeyColumn != null)
                throw new RedshiftCommandBuilderException($"Column '{sortKeyColumn.Name}' is already marked as a sort key. Only one sort key may be specified if setting sort key on the column level. If you need to specify more than one sort key, define the sort key at the table level.");

            if (ColumnBuilders.Count(x => string.Equals(x.Name, name, StringComparison.InvariantCultureIgnoreCase)) > 0)
                throw new RedshiftCommandBuilderException($"A column with name '{name}' has already been added.");

            ColumnBuilders.Add(columnBuilder);
            return columnBuilder;
        }
    }

    public interface IRedshiftColumnBuilder
    {
        IRedshiftColumnBuilder Identity(int seed, int step);
        IRedshiftColumnBuilder Unique();
        IRedshiftColumnBuilder DistributionKey();
        IRedshiftColumnBuilder SortKey();
        IRedshiftColumnBuilder Nullable();
        IRedshiftColumnBuilder PrimaryKey();
        IRedshiftColumnBuilder References(string tableName, string columnName);
    }

    public class RedshiftColumnBuilder : IRedshiftBuilder, IRedshiftColumnBuilder
    {
        public RedshiftColumnDataTypeDefinition DataTypeDefinition { get; }
        public string Name { get; }
        public bool IsIdentity { get; private set; }
        public bool IsUnique { get; private set; }
        public bool IsDistributionKey { get; private set; }
        public bool IsSortKey { get; private set; }
        public bool IsNullable { get; private set; }
        public bool IsPrimaryKey { get; private set; }
        public bool HasReferenceColumn { get; private set; }
        public string ReferencesTableName { get; private set; }
        public string ReferenceColumnName { get; private set; }
        public int IdentitySeed { get; private set; }
        public int IdentityStep { get; private set; }

        public RedshiftColumnBuilder(string name)
        {
            Name = name;
            DataTypeDefinition = new RedshiftColumnDataTypeDefinition();
        }

        public IRedshiftColumnBuilder Identity(int seed, int step)
        {
            IsIdentity = true;
            IdentitySeed = seed;
            IdentityStep = step;
            return this;
        }

        public IRedshiftColumnBuilder Unique()
        {
            IsUnique = true;
            return this;
        }

        public IRedshiftColumnBuilder DistributionKey()
        {
            IsDistributionKey = true;
            return this;
        }

        public IRedshiftColumnBuilder SortKey()
        {
            IsSortKey = true;
            return this;
        }

        public IRedshiftColumnBuilder Nullable()
        {
            IsNullable = true;
            return this;
        }

        public IRedshiftColumnBuilder PrimaryKey()
        {
            IsUnique = false;
            IsPrimaryKey = true;
            return this;
        }

        public IRedshiftColumnBuilder References(string tableName, string columnName)
        {
            HasReferenceColumn = true;
            ReferencesTableName = tableName;
            ReferenceColumnName = columnName;
            return this;
        }

        public string Build()
        {
            var sb = new StringBuilder(Name);
            var dataType = DataTypeDefinition.DataType.ToString().ToUpperInvariant();
            switch (DataTypeDefinition.DataType)
            {
                case RedshiftDataType.Int2:
                case RedshiftDataType.Int4:
                case RedshiftDataType.Int8:
                case RedshiftDataType.Float4:
                case RedshiftDataType.Float8:
                case RedshiftDataType.Boolean:
                case RedshiftDataType.Date:
                case RedshiftDataType.Timestamp:
                case RedshiftDataType.TimestampZ:
                    sb.Append($" {dataType}");
                    break;
                case RedshiftDataType.Char:
                case RedshiftDataType.VarChar:
                    sb.Append($" {dataType}({DataTypeDefinition.Length})");
                    break;
                case RedshiftDataType.Decimal:
                    sb.Append($" {dataType}({DataTypeDefinition.Precision},{DataTypeDefinition.Scale})");
                    break;
                default:
                    throw new RedshiftCommandBuilderException($"Unsupported data type '{dataType}' for column '{Name}'.");
            }

            // column attributes
            if (IsIdentity)
                sb.Append($" IDENTITY ({IdentitySeed},{IdentityStep})");
            if (IsDistributionKey)
                sb.Append(" DISTKEY");
            if (IsSortKey)
                sb.Append(" SORTKEY");

            // column constraints
            sb.AppendFormat(" {0}NULL", IsNullable ? "" : "NOT ");
            if (IsPrimaryKey)
                sb.Append(" PRIMARY KEY");
            if (IsUnique)
                sb.Append(" UNIQUE");
            if (HasReferenceColumn)
                sb.Append($" REFERENCES ({ReferencesTableName}({ReferenceColumnName})");

            return sb.ToString();
        }
    }

    public interface IRedshiftColumnDataTypeDefinitionBuilder
    {
        void AsInt2();
        void AsInt4();
        void AsInt8();
        void AsDecimal(int precision, int scale);
        void AsFloat4();
        void AsFloat8();
        void AsBoolean();
        void AsChar(int length);
        void AsVarChar(int length);
        void AsDate();
        void AsTimestamp();
        void AsTimestampZ();

    }

    public enum RedshiftDataType
    {
        Int2,
        Int4,
        Int8,
        Decimal,
        Float4,
        Float8,
        Boolean,
        Char,
        VarChar,
        Date,
        Timestamp,
        TimestampZ
    }

    public class RedshiftColumnDataTypeDefinition : IRedshiftColumnDataTypeDefinitionBuilder
    {
        public RedshiftDataType DataType { get; private set; }
        public int Precision { get; private set; }
        public int Scale { get; private set; }
        public int Length { get; private set; }

        public void AsInt2()
        {
            DataType = RedshiftDataType.Int2;
        }

        public void AsInt4()
        {
            DataType = RedshiftDataType.Int4;
        }

        public void AsInt8()
        {
            DataType = RedshiftDataType.Int8;
        }

        public void AsDecimal(int precision, int scale)
        {
            DataType = RedshiftDataType.Decimal;
            Precision = precision;
            Scale = scale;
        }

        public void AsFloat4()
        {
            DataType = RedshiftDataType.Float4;
        }

        public void AsFloat8()
        {
            DataType = RedshiftDataType.Float8;
        }

        public void AsBoolean()
        {
            DataType = RedshiftDataType.Boolean;
        }

        public void AsChar(int length)
        {
            DataType = RedshiftDataType.Char;
            Length = length;
        }

        public void AsVarChar(int length)
        {
            DataType = RedshiftDataType.VarChar;
            Length = length;
        }

        public void AsDate()
        {
            DataType = RedshiftDataType.Date;
        }

        public void AsTimestamp()
        {
            DataType = RedshiftDataType.Timestamp;
        }

        public void AsTimestampZ()
        {
            DataType = RedshiftDataType.TimestampZ;
        }
    }
}