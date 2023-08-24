using Consensus.Core;
using Consensus.Persistence.Metadata;
using TaskFlux.Core;

namespace Consensus.Storage.File.Tests;

[Trait("Category", "Raft")]
public class FileMetadataStorageTests
{
    private static MemoryStream CreateStream()
    {
        return new MemoryStream(16 + 10);
    }

    private static NodeId? GetNodeId(int? value) => value is null
                                                        ? null
                                                        : new NodeId(value.Value);

    private static Term GetTerm(int value) => new Term(value);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(100)]
    [InlineData(12323)]
    [InlineData(132323)]
    public void GetTerm__КогдаЛогБылПуст__ДолженВернутьПереданныйТермПоУмолчанию(int term)
    {
        using var memory = CreateStream();
        var expected = new Term(term);
        var storage = new FileMetadataStorage(memory, expected, null);

        var actual = storage.ReadTerm();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(1240)]
    public void GetVotedFor__КогдаЛогПуст__ДолженВернутьЗначениеПоУмолчанию(int? votedFor)
    {
        using var memory = CreateStream();
        NodeId? expected = votedFor is null
                               ? null
                               : new NodeId(votedFor.Value);
        var storage = new FileMetadataStorage(memory, Term.Start, expected);

        var actual = storage.ReadVotedFor();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(1, 1)]
    [InlineData(3, 1)]
    [InlineData(3, 6)]
    [InlineData(2, 3)]
    [InlineData(3, 10)]
    public void Update__КогдаЛогБылПуст__ДолженОбновитьТерм(int defaultTerm, int newTerm)
    {
        using var memory = CreateStream();
        var storage = new FileMetadataStorage(memory, new Term(defaultTerm), null);
        var expected = new Term(newTerm);

        storage.Update(expected, null);

        var actual = storage.ReadTerm();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(null, 1)]
    [InlineData(null, null)]
    [InlineData(1, null)]
    [InlineData(2, null)]
    [InlineData(2, 1)]
    [InlineData(2, 10)]
    [InlineData(10, 10)]
    [InlineData(10, null)]
    [InlineData(10, 23)]
    [InlineData(3, 2)]
    [InlineData(1, 2)]
    [InlineData(1, 1)]
    public void Update__КогдаЛогБылПуст__ДолженОбновитьГолос(int? defaultVotedFor, int? newVotedFor)
    {
        using var memory = CreateStream();
        var expected = GetNodeId(newVotedFor);
        var storage = new FileMetadataStorage(memory, Term.Start, GetNodeId(defaultVotedFor));

        storage.Update(Term.Start, expected);

        var actual = storage.ReadVotedFor();
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(100, 123)]
    [InlineData(100, 1323223)]
    [InlineData(987654, 1323223)]
    [InlineData(12323, 1)]
    [InlineData(132323, 34)]
    [InlineData(Term.StartTerm, int.MaxValue)]
    public void GetTerm__КогдаЛогНеБылПуст__ДолженВернутьХранившийсяТерм(int defaultTerm, int newDefaultTerm)
    {
        using var memory = CreateStream();
        var expected = GetTerm(defaultTerm);
        var oldStorage = new FileMetadataStorage(memory, expected, null);

        var _ = oldStorage.ReadTerm();
        var newStorage = new FileMetadataStorage(memory, GetTerm(newDefaultTerm), null);
        var actual = newStorage.ReadTerm();

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(null, 1)]
    [InlineData(1, null)]
    [InlineData(null, null)]
    [InlineData(2, 4)]
    [InlineData(100, 123)]
    [InlineData(100, 1323223)]
    [InlineData(int.MaxValue, null)]
    [InlineData(1231235653, 1)]
    [InlineData(987654, 1)]
    [InlineData(12323, 1)]
    [InlineData(null, int.MaxValue)]
    [InlineData(2, int.MaxValue)]
    public void GetVotedFor__КогдаЛогНеБылПуст__ДолженВернутьХранившийсяГолос(
        int? defaultVotedFor,
        int? newDefaultVotedFor)
    {
        using var memory = CreateStream();
        var expected = GetNodeId(defaultVotedFor);
        var oldStorage = new FileMetadataStorage(memory, Term.Start, expected);

        var _ = oldStorage.ReadVotedFor();
        var newStorage = new FileMetadataStorage(memory, Term.Start, GetNodeId(newDefaultVotedFor));
        var actual = newStorage.ReadVotedFor();

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1, 2, 3)]
    [InlineData(1, 54, 1)]
    [InlineData(10, 2345, 1)]
    [InlineData(10, 35347546, 23233)]
    [InlineData(67, 35347546, 1)]
    [InlineData(67, int.MaxValue, 1)]
    [InlineData(1, int.MaxValue, 100)]
    [InlineData(1, int.MaxValue, int.MaxValue)]
    [InlineData(int.MaxValue, 1, 435)]
    [InlineData(int.MaxValue, int.MaxValue - 1, int.MaxValue - 2)]
    [InlineData(int.MaxValue - 2, int.MaxValue - 1, int.MaxValue)]
    [InlineData(Term.StartTerm, 2, Term.StartTerm)]
    [InlineData(Term.StartTerm, 10, Term.StartTerm)]
    [InlineData(Term.StartTerm, 1243534634, Term.StartTerm)]
    [InlineData(Term.StartTerm, int.MaxValue, Term.StartTerm)]
    public void GetTerm__КогдаБылВызыванUpdate__ВНовомЛогеСТемЖеПотокомДолженВернутьЗаписанныйТерм(
        int defaultTerm,
        int setTerm,
        int newDefaultTerm)
    {
        using var memory = CreateStream();
        var expected = GetTerm(setTerm);
        var oldStorage = new FileMetadataStorage(memory, GetTerm(defaultTerm), null);

        oldStorage.Update(expected, null);
        var newStorage = new FileMetadataStorage(memory, GetTerm(newDefaultTerm), null);
        var actual = newStorage.ReadTerm();

        Assert.Equal(expected, actual);
    }
}