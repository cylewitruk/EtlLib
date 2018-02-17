# EtlLib: A Simple ETL Framework for .NET

> Note: This readme is still being updated as of 2018-01-30 :)

EtlLib is a small, lightweight and simple ETL (Extract-Transform-Load) framework for .NET, targeting .NET Standard 2.0, which aims to make life a little bit easier (and declarative) for simpler ETL tasks.  For heavy-lifting and complex inter-dependent ETL processes you should probably still be looking at the heavy-lifters such as Pentaho or Jasper.

#### Goals of the Project:

- Provide a declarative DSL which is easy to understand and code-review.
- Provide reusable components for common tasks.
- Achieve a reasonable level of performance.
- Strive for extensibility and introduce functionality via integration libraries, while providing enough base functionality to keep those integrations simple and straightforward.
- Should be easy to troubleshoot and identify problems.

#### Current Version

The current version of this project is *beta*.  I have used it in simpler scenarios, but not all paths are fully tested and I'm working on getting the test coverage up-to-par.

#### Packages

| Package                      | Description                              |     State     |
| ---------------------------- | ---------------------------------------- | :-----------: |
| EtlLib                       | Core library and base functionality of EtlLib. | Mostly Tested |
| EtlLib.Logging.NLog          | NLog logging adapter for EtlLib.         |      OK       |
| EtlLib.Nodes.AmazonS3        | Amazon S3 integration for EtlLib.        |      OK       |
| EtlLib.Nodes.CsvFiles        | Integration with CsvHelper library for reading and writing CSV files. |      OK       |
| EtlLib.Nodes.Dapper          | Integration with Dapper for reading typed data from supported databases. | Mostly Tested |
| EtlLib.Nodes.FileCompression | Integration with SharpZipLib for compressing files. | Mostly Tested |
| EtlLib.Nodes.MongoDb         | Integration with MongoDB's official driver for reading and writing documents to MongoDB. |  Not Tested   |
| EtlLib.Nodes.PostgreSQL      | Integration with Npgsql (official .NET driver) for reading and writing data to PostgreSQL. |  Not Tested   |
| EtlLib.Nodes.Redshift        | Integration with Amazon Redshift, using Npgsql.  Supports general ETL processes, such as creating staging tables, running the COPY command, etc. |  Not Tested   |
| EtlLib.Nodes.SqlServer       | Integration with Microsoft's SqlServerConnection for reading and writing to MSSQL databases. |  Not Tested   |



#### A Quick Example:

In this example, we create an **ETL Process** which reads a CSV file, generates row numbers for the data, adds a new column *income segment* with classification information, filters away items where *grade* == *"A"*, adds a new column *is_transformed* with the value *true*, dumps the result to a new CSV file and BZip2's up the results.

```c#
    var builder = EtlProcessBuilder.Create()
        .Input(ctx => new CsvReaderNode(@"C:\Files\LoanStats3a.csv"))
        .GenerateRowNumbers("_id")
        .Classify("income_segment", cat =>
        {
            decimal Income(Row row) => row.GetAs<decimal>("annual_inc");
            cat
              .When(x => string.IsNullOrWhiteSpace(x.GetAs<string>("annual_inc")), "UNKNOWN")
              .When(x => Income(x) < 10000L, "0-9999")
              .When(x => Income(x) < 20000L, "10000-19999")
              .When(x => Income(x) < 30000L, "20000-29999")
              .Default("30000+");
      })
      .Filter(row => row.GetAs<string>("grade") == "A")
      .Transform((ctx, row) =>
      {
          var newRow = ctx.ObjectPool.Borrow<Row>();
          row.CopyTo(newRow);
          newRow["is_transformed"] = true;
          ctx.ObjectPool.Return(row);
          return newRow;
      })
      .Continue(ctx => new CsvWriterNode(@"C:\Files\LoanStats3a_TRANSFORMED.csv"))
          .IncludeHeader()
          .WithEncoding(Encoding.UTF8))
      .BZip2Files()
      .CompleteWithResult();
```
In this example, `GenerateRowNumbers()`, `Classify()`, `Filter()`, `Transform()` are extension methods providing a cleaner DSL for built-in nodes `GenericTransformationNode<T>`, `GenericFilterNode<T>` and `GenericClassificationNode<T>`, which could have also been declared manually with the `.Continue()` syntax.  `Bzip2Files()` is an extension method from the *EtlLib.Nodes.FileCompression* package.



## The Building Blocks

### ETL Pipeline

The primary concept of EtlLib is the **ETL Pipeline** and its **ETL Operations**.  ETL Pipelines execute step-by-step, synchronously by default, although you can use the `.RunParallel()` method to run several operations in tandem.

ETL Pipelines also take care of **Object Pooling**, if desired, which can help with GC-thrashing when a large number of records will be processed by re-using objects and keeping them referenced instead of constantly creating new objects for each input record and leaving them for the GC to clean up.

### ETL Pipeline Context

The **ETL Pipeline Context** is an object which follows along throughout the execution of the pipeline.  It's available to you from the beginning where you can load configuration parameters and save state along the way.  ETL Pipeline Contexts are thread-safe.

ETL Pipeline Contexts can be created either declaratively for sharing configuration across multiple ETL Processes, or implicitly during the creation of an ETL Pipeline.

#### Declarative Example

```C#
// Note: This could be populated from database, configuration file, etc.
var config = new EtlPipelineConfig()
    .Set("s3_bucket_name", "my-bucket")
    .SetAmazonS3BasicCredentials("someaccesskeyid", "somesecretaccesskey")
    .Set("output_dir", @"C:\Files\etl_test\");

var context = new EtlPipelineContext(config);
context.State["hello"] = "world!";

EtlPipeline.Create(context)...
```

#### Implicit/Inline Example

```c#
EtlPipeline.Create(settings =>
{
    settings.WithConfig(cfg => cfg
        .Set("s3_bucket_name", "my-bucket")
        .SetAmazonS3BasicCredentials("someaccesskeyid", "somesecretaccesskey")
        .Set("output_dir", @"C:\Files\etl_test\"));

    settings.WithContextInitializer(ctx => ctx.State["hello"] = "world!");
})...
```

And then the context will be available to you when calling methods such as `Run(ctx => ...)`.

> Note: If you are designing your entire ETL Pipeline in one class, making use of closures are probably an easier way to go.  However, if you are designing larger ETL Pipelines split across several files, the context provides a simple way of making data available throughout the pipeline.

#### Executing an ETL Pipeline

ETL Pipelines are lazy.  You must explicitly execute the Pipeline, which means that Pipelines can be stashed away in a variable for later execution:

```c#
var pipeline = EtlPipeline.Create(settings => {})
    .Run(ctx => 
         new DynamicInvokeEtlOperation(new Action(() => Debug.WriteLine("Hello World!")))));

var result = pipeline.Execute();
```

Or alternatively, completely fluently:

```c#
var result = EtlPipeline.Create(settings => {})
    .Run(ctx => 
         new DynamicInvokeEtlOperation(new Action(() => Debug.WriteLine("Hello World!")))))
    .Execute();
```

### ETL Operation

An **ETL Operation** are the essential building blocks of an **ETL Pipeline**.  ETL Pipelines only execute ETL Operations, and there are three types of ETL Operations:

- No Result Operations: operations which perform an action but do not return any result other than success/fail.
- Scalar Result Operations: operations which perform an action and return a singular, scalar result in addition to success/fail.
- Enumerable Result Operations: operations which perform an action and return an enumerable result in addition to success/fail.

ETL Operations are run by the pipeline by using one of the available `Run()` or `RunParallel()` methods on the ETL Pipeline:

```c#
EtlPipeline.Create(settings => {})
    .Run(ctx => 
         new DynamicInvokeEtlOperation(new Action(() => Debug.WriteLine("Hello World!"))))
    .RunParallel(ctx => new[]
    {
        new DynamicInvokeEtlOperation(new Action(() => Debug.WriteLine("Hello"))),
        new DynamicInvokeEtlOperation(new Action(() => Debug.WriteLine("World!")))
    });
```

### ETL Process

The **ETL Process** is a built-in **ETL Operation** either of **No Result** or **Enumerable Result** type, which provides streaming-style ETL via **Nodes**.  This can be useful when trying to keep down memory usage - remember that ETL Operations execute synchronously, one-by-one, so that's not the best place to be processing large streams of data because it will need to be buffered in memory between operations.

By leveraging **Object Pooling** from the ETL Pipeline, one can limit the number of in-flight objects in use (a.k.a. throttling the input) as well as optimize garbage collection by re-using objects.

The first example on this page, **A Quick Example**, is a concrete illustration of how an ETL Process can be defined using an EtlProcessBuilder, but for the sake of brevity:

```c#
var process = EtlProcessBuilder.Create()
    .Input(...) // Will always begin with an Input statement (input to pipeline)
    .Continue(...) // Followed (optionally) by one...
    .Continue(...) // ...or several... Continue statements (input + output)
    .Complete() // Complete without providing results back to the Pipeline (end)
    .CompleteWithResult() // Complete and pass results back to the Pipeline (end with output)
```

Alternatively, an ETL Process may be defined declaratively in its own class, for example this ETL Process which generates a date dimension:

```c#
public class GenerateDateDimensionEtlProcess : AbstractEtlProcess<NodeOutputWithFilePath>
{
    private static readonly Calendar Calendar;

    static GenerateDateDimensionEtlProcess()
    {
        Calendar = new GregorianCalendar();
    }

public GenerateDateDimensionEtlProcess(string outputFilePath)
{
    var startDate = new DateTime(2000, 1, 1, 0, 0, 0);
    var endDate = new DateTime(2025, 0, 0, 0, 0, 0);

    Build(builder =>
    {
        builder
            .Named("Generate Date Dimension")
            .GenerateInput<Row, DateTime>(
                gen => gen.State <= endDate,
                (ctx, i, gen) =>
                {
                    if (i == 1)
                        gen.SetState(startDate);

                    var row = ctx.ObjectPool.Borrow<Row>();
                    CreateRowFromDateTime(row, gen.State);
                    gen.SetState(gen.State.AddDays(1));

                    return row;
                }
            )
            .Continue(ctx => new CsvWriterNode(outputFilePath)
                .IncludeHeader()
                .WithEncoding(Encoding.UTF8))
            .BZip2Files(cfg => cfg
                .CompressionLevel(9)
                .Parallelize(2)
                .FileSuffix(".bzip2"))
            .CompleteWithResult();
});
```
Which can in turn be used in an ETL Pipeline:

```c#
var pipelineResult = EtlPipeline.Create(cfg => cfg
    .Named("Test ETL Process")
    .UseExistingContext(context))
    .EnsureDirectoryTreeExists(ctx => ctx.Config["output_dir"])
    .Run(ctx => new GenerateDateDimensionEtlProcess(Path.Combine(ctx.Config["output_dir"], "dates.csv")),
        result => result.AppendResult(filesToUploadToS3))
    .Run(ctx => new AmazonS3WriterEtlOperation(RegionEndpoint.EUWest1, ctx.Config["s3_bucket_name"], filesToUploadToS3.Select(x => x.FilePath))
        .WithStorageClass(S3StorageClass.ReducedRedundancy), 
        result => result.ForEachResult((ctx, i, item) => Console.WriteLine($"S3 Result: {item.ObjectKey} {item.ETag}"))
)
.Execute();
```
### Nodes

**Nodes** are a built-in feature which are used by **ETL Processes** and are designed for processing data in a streaming fashion.  Each node in an ETL Process runs in its own thread (scheduled by the task scheduler) with an input/output adapter sitting in-between which *Nodes with Output* **Emit** to and *Nodes with Input* **Consume** from.

Data is passed between nodes as objects.  These objects as a rule should be immutable, and must implement the `INodeOutput<T>` interface which in turn implements the `IFreezable` (immutability) and `IResettable` (object pooling) interfaces, be a class and have a public parameterless constructor.  When a record passes through the input/output adapter, it will call `IFreezable.Freeze()` on the record.  If the framework decides that it needs to clone the object (in a branching scenario, for example), it will call`INodeOutput<T>.CopyTo()` on the record, so if you implement your own objects, be sure to implement these methods appropriately.

EtlLib provides two implementations of `INodeOutput<T>`: `Row` and `NodeOutputWithFilePath`.

**Row** is a simple dictionary-type with string keys and object values.  It is the default output format for a number of the framework-provided nodes, such as `CsvReaderNode` and `[Db]ReaderNode`, for example.

**NodeOutputWithFilePath** is a type which implements `IHasFilePath`.  A number of nodes, such as nodes from *EtlLib.Nodes.FileCompression* or *EtlLib.Nodes.AmazonS3* will take results containing file paths, after for example a `CsvWriterNode` has produced one or more files, and take action on them.  This output type is used to provide a consistent way of communicating file paths between nodes.

## More on the way....

Soon!