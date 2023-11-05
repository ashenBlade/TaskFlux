using Consensus.Raft;
using Consensus.Raft.Persistence.Metadata;
using TaskFlux.Models;

namespace Consensus.Storage.Tests;

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
    public void Term__КогдаЛогБылПуст__ДолженВернутьПереданныйТермПоУмолчанию(int term)
    {
        using var memory = CreateStream();
        var expected = new Term(term);
        var storage = new FileMetadataStorage(memory, expected, null);

        var actual = storage.Term;
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(1240)]
    public void VotedFor__КогдаЛогПуст__ДолженВернутьЗначениеПоУмолчанию(int? votedFor)
    {
        using var memory = CreateStream();
        NodeId? expected = votedFor is null
                               ? null
                               : new NodeId(votedFor.Value);
        var storage = new FileMetadataStorage(memory, Term.Start, expected);

        var actual = storage.VotedFor;
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(1, 2, null, 1)]
    [InlineData(1, 1, 1, 1)]
    [InlineData(3, 1, 1, 1)]
    [InlineData(123, 213, 22222, 222)]
    [InlineData(2, 234, null, null)]
    [InlineData(3, 10, int.MaxValue, int.MaxValue)]
    [InlineData(2, 1, 323, 9876543)]
    public void Update__КогдаЛогБылПуст__ДолженОбновитьДанные(int defaultTerm,
                                                              int newTerm,
                                                              int? defaultVotedFor,
                                                              int? newVotedFor)
    {
        using var memory = CreateStream();
        var storage = new FileMetadataStorage(memory, new Term(defaultTerm), ToNodeId(defaultVotedFor));
        var expectedTerm = new Term(newTerm);
        var expectedVotedFor = ToNodeId(newVotedFor);

        storage.Update(expectedTerm, expectedVotedFor);

        // Проверяем кэширование
        var actualTerm = storage.Term;
        var actualVotedFor = storage.VotedFor;
        Assert.Equal(expectedTerm, actualTerm);
        Assert.Equal(expectedVotedFor, actualVotedFor);

        // Проверяем данные с диска
        var (readTerm, readVotedFor) = storage.ReadStoredDataTest();
        Assert.Equal(expectedTerm, readTerm);
        Assert.Equal(expectedVotedFor, readVotedFor);

        NodeId? ToNodeId(int? value) => value is { } v
                                            ? new NodeId(v)
                                            : null;
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
        var expectedVotedFor = GetNodeId(newVotedFor);
        var storage = new FileMetadataStorage(memory, Term.Start, GetNodeId(defaultVotedFor));

        storage.Update(Term.Start, expectedVotedFor);

        var (_, actualVotedFor) = storage.ReadStoredDataTest();
        Assert.Equal(expectedVotedFor, actualVotedFor);
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
    public void Term__КогдаБылВызыванUpdate__ВНовомЛогеСТемЖеФайломДолженВернутьЗаписанныйТерм(
        int defaultTerm,
        int setTerm,
        int newDefaultTerm)
    {
        using var memory = CreateStream();
        var expectedTerm = GetTerm(setTerm);
        var oldStorage = new FileMetadataStorage(memory, GetTerm(defaultTerm), null);

        oldStorage.Update(expectedTerm, null);

        var newStorage = new FileMetadataStorage(memory, GetTerm(newDefaultTerm), null);

        var (actualTerm, _) = newStorage.ReadStoredDataTest();
        Assert.Equal(expectedTerm, actualTerm);
    }
}