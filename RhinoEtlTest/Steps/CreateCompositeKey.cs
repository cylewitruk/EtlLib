using System.Collections.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;

namespace RhinoEtlTest.Steps
{
    public class CreateCompositeKey : AbstractOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                row["composite_key"] = $"{row["source"]}-{row["sequence_number"]}";
                yield return row;
            }
        }
    }
}