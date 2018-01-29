using Xunit.Abstractions;

namespace EtlLib.UnitTests
{
    public abstract class TestBase
    {
        protected ITestOutputHelper TestOutput { get; }

        protected TestBase(ITestOutputHelper output)
        {
            TestOutput = output;
        }
    }
}