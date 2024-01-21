using System.IO.Abstractions;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Consensus.Persistence.Snapshot;
using TaskFlux.Consensus.Tests.Infrastructure;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Persistence")]
public class SnapshotFileTests
{
    private IDirectoryInfo _dataDirectory;

    private SnapshotFile CreateSnapshot()
    {
        var fs = Helpers.CreateFileSystem();
        _dataDirectory = fs.DataDirectory;
        return SnapshotFile.Initialize(fs.DataDirectory);
    }

    [Fact]
    public void LastApplied__КогдаСнапшотаНет__ДолженВернутьTomb()
    {
        var snapshot = CreateSnapshot();

        var actual = snapshot.LastApplied;

        Assert.Equal(LogEntryInfo.Tomb, actual);
    }

    [Fact]
    public void TryGetSnapshot__КогдаСнапшотаНет__ДолженВернутьFalse()
    {
        var snapshot = CreateSnapshot();

        var actual = snapshot.TryGetSnapshot(out _, out _);

        Assert.False(actual);
    }

    [Fact]
    public void TryGetSnapshot__КогдаСнапшотЕсть__ДолженВернутьХранившийсяСнапшот()
    {
        var snapshot = CreateSnapshot();
        var expected = "asdfasdfasdfasdfasdfasdfas8i3g1-qabee3rqopby8i-q3489yq3a-8"u8.ToArray();
        snapshot.SetupSnapshotTest(1, 1, new StubSnapshot(expected));

        snapshot.TryGetSnapshot(out var actualSnapshot, out _);
        var actual = ReadSnapshotFull(actualSnapshot);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CreateTempSnapshotFile__КогдаСнапшотаНеБыло__ДолженСоздатьНовыйФайлСнапшотаКорректно()
    {
        var snapshot = CreateSnapshot();
        var data = Enumerable.Range(0, 1000)
                             .Select(i => ( byte ) ( i % byte.MaxValue ))
                             .ToArray();

        var tempSnapshotFile = snapshot.CreateTempSnapshotFile();
        tempSnapshotFile.Initialize(new LogEntryInfo(1, 0));
        foreach (var chunk in data.Chunk(100))
        {
            tempSnapshotFile.WriteSnapshotChunk(chunk, CancellationToken.None);
        }

        tempSnapshotFile.Save();

        var (_, _, actual) = snapshot.ReadAllDataTest();
        Assert.Equal(data, actual);

        var ex = Record.Exception(() => SnapshotFile.Initialize(_dataDirectory));
        Assert.Null(ex);
    }

    [Fact]
    public void CreateTempSnapshotFile__КогдаСнапшотаНеБыло__ДолженОбновитьДанныеОПоследнейКоманде()
    {
        var snapshot = CreateSnapshot();
        var data = Enumerable.Range(0, 1000)
                             .Select(i => ( byte ) ( i % byte.MaxValue ))
                             .ToArray();
        var expected = new LogEntryInfo(120, 2222);
        var tempSnapshotFile = snapshot.CreateTempSnapshotFile();
        tempSnapshotFile.Initialize(expected);
        foreach (var chunk in data.Chunk(100))
        {
            tempSnapshotFile.WriteSnapshotChunk(chunk, CancellationToken.None);
        }

        tempSnapshotFile.Save();

        var (index, term, actual) = snapshot.ReadAllDataTest();
        Assert.Equal(expected, new LogEntryInfo(term, index));

        var ex = Record.Exception(() =>
        {
            var sf = SnapshotFile.Initialize(_dataDirectory);
            Assert.Equal(expected, sf.LastApplied);
        });
        Assert.Null(ex);
    }

    private byte[] ReadSnapshotFull(ISnapshot snapshot)
    {
        var memory = new MemoryStream();
        foreach (var readOnlyMemory in snapshot.GetAllChunks())
        {
            memory.Write(readOnlyMemory.Span);
        }

        return memory.ToArray();
    }
}