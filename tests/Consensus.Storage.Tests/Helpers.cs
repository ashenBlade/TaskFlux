using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using Consensus.Raft;
using Consensus.Raft.Persistence;
using Consensus.Raft.Persistence.Log;
using Moq;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Storage.Tests;

public static class Helpers
{
    public static readonly FileLogStorage NullStorage = new(null!);

    private static ILogStorage CreateNullStorage()
    {
        return new Mock<ILogStorage>(MockBehavior.Loose).Object;
    }

    private static readonly string WorkingDirectory = Path.Combine("/var", "lib", "taskflux");
    private static readonly string ConsensusDirectory = Path.Combine(WorkingDirectory, "consensus");

    public static (MockFileSystem, IFileInfo SnapshotFile, IDirectoryInfo TemporaryDirectory) CreateBaseMockFileSystem(
        bool createEmptySnapshotFile = true)
    {
        var snapshotFilePath = Path.Combine(ConsensusDirectory, "raft.snapshot");
        var tempDirPath = Path.Combine(WorkingDirectory, "temporary");
        var fs = new MockFileSystem(
            new Dictionary<string, MockFileData>()
            {
                {ConsensusDirectory, new MockDirectoryData()},
                {Path.Combine(ConsensusDirectory, "raft.log"), new MockFileData(Array.Empty<byte>())},
                {Path.Combine(ConsensusDirectory, "raft.metadata"), new MockFileData(Array.Empty<byte>())},
                {snapshotFilePath, new MockFileData(Array.Empty<byte>())},
                {tempDirPath, new MockDirectoryData()}
            }, new MockFileSystemOptions() {CurrentDirectory = WorkingDirectory, CreateDefaultTempDir = false});

        if (!createEmptySnapshotFile)
        {
            fs.RemoveFile(snapshotFilePath);
        }

        return ( fs, fs.FileInfo.New(snapshotFilePath), fs.DirectoryInfo.New(tempDirPath) );
    }


    public static (LogEntryInfo Entry, byte[] Data) ReadSnapshot(Stream file)
    {
        var reader = new StreamBinaryReader(file);
        // 1. Заголовок
        var header = reader.ReadInt32();
        Assert.Equal(Constants.Marker, header);

        // 2. Мета
        var index = reader.ReadInt32();
        var term = reader.ReadInt32();

        // 3. Данные
        var data = reader.ReadBuffer();
        return ( new LogEntryInfo(new Term(term), index), data );
    }
}