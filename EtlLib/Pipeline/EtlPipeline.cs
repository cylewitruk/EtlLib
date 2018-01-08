using EtlLib.Logging;

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

        public static IEtlPipeline Create()
        {
            return new EtlPipeline();
        }
    }
}