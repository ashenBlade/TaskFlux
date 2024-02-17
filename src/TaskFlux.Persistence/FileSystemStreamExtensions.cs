using System.Diagnostics;
using System.IO.Abstractions;

namespace TaskFlux.Persistence;

public static class FileSystemStreamExtensions
{
    public static void Fsync(this FileSystemStream stream)
    {
        var watch = Stopwatch.StartNew();
        try
        {
            stream.Flush(true);
        }
        finally
        {
            watch.Stop();
            Metrics.FsyncDuration.Record(watch.ElapsedMilliseconds);
        }
    }
}