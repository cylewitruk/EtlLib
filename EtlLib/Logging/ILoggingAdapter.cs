namespace EtlLib.Logging
{
    public interface ILoggingAdapter
    {
        ILogger CreateLogger(string name);
    }
}