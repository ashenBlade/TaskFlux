using System.IO.Abstractions;
using TaskFlux.Consensus;
using TaskFlux.Core;
using TaskFlux.Persistence.Metadata;
using Xunit;

namespace TaskFlux.Persistence.Tests;

[Trait("Category", "Persistence")]
public class MetadataFileTests
{
    private static NodeId? GetNodeId(int? value) => value is null
                                                        ? null
                                                        : new NodeId(value.Value);

    private static Term GetTerm(int value) => new Term(value);

    /// <summary>
    /// Директория с файлами данных.
    /// Инициализируется когда вызывается <see cref="CreateFile"/>
    /// </summary>
    private IDirectoryInfo _dataDirectory = null!;

    private MetadataFile CreateFile((Term Term, NodeId? VotedFor)? defaultValues = null)
    {
        var fs = Helpers.CreateFileSystem();
        var file = MetadataFile.Initialize(fs.DataDirectory);
        if (defaultValues is var (term, votedFor))
        {
            file.SetupMetadataTest(term, votedFor);
        }

        _dataDirectory = fs.DataDirectory;
        return file;
    }

    [Fact]
    public void Term__КогдаЛогБылПуст__ДолженВернутьТермПоУмолчанию()
    {
        var fs = Helpers.CreateFileSystem();
        var expected = MetadataFile.DefaultTerm;
        var storage = MetadataFile.Initialize(fs.DataDirectory);

        var actual = storage.Term;

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void VotedFor__КогдаЛогПуст__ДолженВернутьNull()
    {
        var fs = Helpers.CreateFileSystem();
        var storage = MetadataFile.Initialize(fs.DataDirectory);

        var actual = storage.VotedFor;

        Assert.Null(actual);
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
        var file = CreateFile(( defaultTerm, ToNodeId(defaultVotedFor) ));
        var expectedTerm = new Term(newTerm);
        var expectedVotedFor = ToNodeId(newVotedFor);

        file.Update(expectedTerm, expectedVotedFor);

        // Проверяем кэширование
        var actualTerm = file.Term;
        var actualVotedFor = file.VotedFor;
        Assert.Equal(expectedTerm, actualTerm);
        Assert.Equal(expectedVotedFor, actualVotedFor);

        // Проверяем данные с диска
        var (readTerm, readVotedFor) = file.ReadStoredDataTest();
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
        var expectedVotedFor = GetNodeId(newVotedFor);

        var storage = CreateFile(( Term.Start, GetNodeId(defaultVotedFor) ));

        storage.Update(Term.Start, expectedVotedFor);

        var (_, actualVotedFor) = storage.ReadStoredDataTest();
        Assert.Equal(expectedVotedFor, actualVotedFor);
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(1, 54)]
    [InlineData(10, 2345)]
    [InlineData(10, 35347546)]
    [InlineData(67, 35347546)]
    [InlineData(67, int.MaxValue)]
    [InlineData(1, int.MaxValue)]
    [InlineData(int.MaxValue, 1)]
    [InlineData(int.MaxValue, int.MaxValue - 1)]
    [InlineData(int.MaxValue - 2, int.MaxValue - 1)]
    [InlineData(Term.StartTerm, 10)]
    [InlineData(Term.StartTerm, 1243534634)]
    public void Term__КогдаБылВызванUpdate__ВНовомЛогеСТемЖеФайломДолженВернутьЗаписанныйТерм(
        int defaultTerm,
        int setTerm)
    {
        var oldFile = CreateFile(( GetTerm(defaultTerm), null ));
        var expectedTerm = GetTerm(setTerm);

        oldFile.Update(expectedTerm, null);

        var newStorage = MetadataFile.Initialize(_dataDirectory);

        var (actualTerm, _) = newStorage.ReadStoredDataTest();
        Assert.Equal(expectedTerm, actualTerm);
    }
}