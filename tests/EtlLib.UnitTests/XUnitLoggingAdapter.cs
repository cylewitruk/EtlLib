using System;
using EtlLib.Logging;
using Xunit.Abstractions;

namespace EtlLib.UnitTests
{
    public class XUnitLoggingAdapter : ILoggingAdapter
    {
        private readonly ITestOutputHelper _output;

        public XUnitLoggingAdapter(ITestOutputHelper output)
        {
            _output = output;
        }

        public ILogger CreateLogger(string name)
        {
            return new XUnitLogger(_output);
        }
    }

    public class XUnitLogger : ILogger
    {
        private readonly ITestOutputHelper _output;

        public XUnitLogger(ITestOutputHelper output)
        {
            _output = output;
        }

        private void WriteLine(string s) => _output.WriteLine(s);

        public void Trace(string s) => WriteLine(s);
        public void Debug(string s) => WriteLine(s);
        public void Info(string s) => WriteLine(s);
        public void Warn(string s) => WriteLine(s);
        public void Error(string s) => WriteLine(s);
        public void Error(string s, Exception e)
        {
            WriteLine(s + ": " + e.Message + "\n" + e.StackTrace);
        }
    }
}