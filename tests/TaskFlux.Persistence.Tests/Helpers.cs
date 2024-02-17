using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using TaskFlux.Consensus.Persistence;

namespace TaskFlux.Persistence.Tests;

public static class Helpers
{
    // Это будут относительные пути
    private static readonly string BaseDirectory = Path.Combine("var", "lib", "taskflux");
    private static readonly string DataDirectory = Path.Combine(BaseDirectory, "data");

    public static (MockFileSystem Fs, IDirectoryInfo Log, IFileInfo Metadata, IFileInfo Snapshot, IDirectoryInfo
        TemporaryDirectory, IDirectoryInfo DataDirectory) CreateFileSystem()
    {
        var fs = new MockFileSystem(new Dictionary<string, MockFileData>() {[DataDirectory] = new MockDirectoryData()});

        var dataDirectory = fs.DirectoryInfo.New(DataDirectory);
        var log = fs.DirectoryInfo.New(Path.Combine(DataDirectory, Constants.LogDirectoryName));
        var metadata = fs.FileInfo.New(Path.Combine(DataDirectory, Constants.MetadataFileName));
        var snapshot = fs.FileInfo.New(Path.Combine(DataDirectory, Constants.SnapshotFileName));
        var tempDirectory = fs.DirectoryInfo.New(Path.Combine(DataDirectory, Constants.TemporaryDirectoryName));

        log.Create();
        metadata.Create();
        snapshot.Create();
        tempDirectory.Create();
        dataDirectory.Create();

        return ( fs, log, metadata, snapshot, tempDirectory, dataDirectory );
    }
}