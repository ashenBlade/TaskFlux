namespace Consensus.Storage.File.Snapshot;

public class FileSystemTemporarySnapshotFileFactory : ITemporarySnapshotFileFactory
{
    /// <summary>
    /// Директория, хранящая временные файлы снапшотов перед тем, как заменить старый снапшот
    /// </summary>
    private readonly DirectoryInfo _temporarySnapshotsDirectory;

    /// <summary>
    /// Объект файла снапшота
    /// </summary>
    private readonly FileInfo _snapshotFile;

    public FileSystemTemporarySnapshotFileFactory(DirectoryInfo temporarySnapshotsDirectory, FileInfo snapshotFile)
    {
        _temporarySnapshotsDirectory = temporarySnapshotsDirectory;
        _snapshotFile = snapshotFile;
    }

    private static string CreateTemporarySnapshotFileName()
    {
        return Path.GetRandomFileName();
    }

    public ITemporarySnapshotFile CreateTempSnapshotFile()
    {
        if (!_temporarySnapshotsDirectory.Exists)
        {
            _temporarySnapshotsDirectory.Create();
        }

        var fullPath = Path.Combine(_temporarySnapshotsDirectory.FullName, CreateTemporarySnapshotFileName());
        var temporaryFile = new FileInfo(fullPath);

        return new FileSystemTemporarySnapshotFile(_snapshotFile, temporaryFile);
    }
}