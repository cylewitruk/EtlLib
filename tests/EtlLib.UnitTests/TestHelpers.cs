using EtlLib.Pipeline;

namespace EtlLib.UnitTests
{
    public static class TestHelpers
    {
        public static EtlPipelineContext CreatePipelineContext()
        {
            return new EtlPipelineContext();
        }
    }
}