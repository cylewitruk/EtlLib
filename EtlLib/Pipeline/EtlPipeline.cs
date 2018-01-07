using EtlLib.Data;
using EtlLib.Logging;
using EtlLib.Nodes;

namespace EtlLib.Pipeline
{
    public class EtlPipeline : IEtlPipeline
    {
        private const string LoggerName = "EtlLib";

        private readonly EtlPipelineContext _context;

        private ILoggingAdapter _loggingAdapter;


        private EtlPipeline()
        {
            _loggingAdapter = new NullLoggerAdapter();
            _context = new EtlPipelineContext(_loggingAdapter.CreateLogger(LoggerName));
        }

        public PipelineResult Execute()
        {
            throw new System.NotImplementedException();
        }

        public IEtlPipeline WithLoggingAdapter(ILoggingAdapter adapter)
        {
            _loggingAdapter = adapter;
            _context.SetLogger(_loggingAdapter.CreateLogger(LoggerName));
            return this;
        }

        private void AttachInputToOutput<T>(INodeWithOutput<T> output, INodeWithInput<T> input) where T : class, IFreezable
        {
            _context.Log.Info($"Attaching input (Id={input.Id}, Type={input.GetType().Name}) to output (Id={output.Id}, Type={output.GetType().Name}).");
            var ioAdapter = new InputOutputAdapter<T>(output);
            ioAdapter.AttachConsumer(input);
        }

        public static IEtlPipeline Create()
        {
            return new EtlPipeline();
        }
    }
}