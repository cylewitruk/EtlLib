using EtlLib.Logging;
using EtlLib.Pipeline.Operations;
using Xunit.Abstractions;

namespace EtlLib.UnitTests
{
    public static class TestHelpers
    {
        public static EtlProcessContext CreateProcessContext(ITestOutputHelper outputHelper = null)
        {
            ILoggingAdapter logAdapter = new NullLoggerAdapter();
            if (outputHelper != null)
                logAdapter = new XUnitLoggingAdapter(outputHelper);

            return new EtlProcessContext(logAdapter);
        }
    }
}