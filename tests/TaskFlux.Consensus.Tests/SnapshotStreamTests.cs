using FluentAssertions;

namespace TaskFlux.Consensus.Tests;

[Trait("Category", "Infrastructure")]
public class SnapshotStreamTests
{
    [Fact]
    public void CopyTo__КогдаСнапшотПуст__ДолженВернутьПустойМассив()
    {
        var stream = new SnapshotStream(new InMemorySnapshot(Array.Empty<byte[]>()));
        var memory = new MemoryStream();

        stream.CopyTo(memory);
        var actual = memory.ToArray();

        actual
           .Should()
           .BeEmpty("снапшот был пуст");
    }

    [Fact]
    public void CopyTo__КогдаВСнапшотеБылТолько1Чанк__ДолженВернутьСодержимоеЭтогоЧанка()
    {
        var data = Enumerable.Range(0, 1000).Select(x => ( byte ) ( x % ( byte.MaxValue + 1 ) )).ToArray();
        var stream = new SnapshotStream(new InMemorySnapshot(new[] {data}));
        var memory = new MemoryStream();

        stream.CopyTo(memory);
        var actual = memory.ToArray();

        actual
           .Should()
           .Equal(data, "прочитанные данные должны быть равными данным в снапшоте");
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public void CopyTo__КогдаВСнапшотеБылоНесколькоЧанков__ДолженВернутьВсеСодержимоеЧанков(int chunksCount)
    {
        const int chunkSize = 100;
        var chunks = Enumerable.Range(0, chunksCount * chunkSize)
                               .Select(i => ( byte ) ( i % ( byte.MaxValue + 1 ) ))
                               .Chunk(chunkSize)
                               .ToArray();
        var expected = chunks.SelectMany(x => x).ToArray();
        var stream = new SnapshotStream(new InMemorySnapshot(chunks));
        var memory = new MemoryStream();

        stream.CopyTo(memory);
        var actual = memory.ToArray();

        actual.Should()
              .Equal(expected, "содержимое потока должно быть равно содержимому снапшота");
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(100, 1)]
    [InlineData(100, 10)]
    [InlineData(100, 99)]
    [InlineData(100, 50)]
    [InlineData(1000, 512)]
    [InlineData(4096, 1024)]
    public void CopyTo__КогдаРазмерБуфераМеньшеРазмераЧанка__ДолженКорректноПрочитатьВсеДанные(
        int chunkSize,
        int bufferSize)
    {
        const int chunksCount = 100;
        var chunks = Enumerable.Range(0, chunksCount * chunkSize)
                               .Select(i => ( byte ) ( i % ( byte.MaxValue + 1 ) ))
                               .Chunk(chunkSize)
                               .ToArray();
        var expected = chunks.SelectMany(x => x).ToArray();
        var stream = new SnapshotStream(new InMemorySnapshot(chunks));
        var memory = new MemoryStream();

        stream.CopyTo(memory, bufferSize);
        var actual = memory.ToArray();

        actual.Should()
              .Equal(expected, "содержимое потока должно быть равно содержимому снапшота");
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(10, 100)]
    [InlineData(100, 101)]
    [InlineData(512, 1024)]
    [InlineData(1024, 4096)]
    [InlineData(4096, 8192)]
    public void CopyTo__КогдаРазмерБуфераБольшеРазмераЧанка__ДолженКорректноПрочитатьВсеДанные(
        int chunkSize,
        int bufferSize)
    {
        const int chunksCount = 100;
        var chunks = Enumerable.Range(0, chunksCount * chunkSize)
                               .Select(i => ( byte ) ( i % ( byte.MaxValue + 1 ) ))
                               .Chunk(chunkSize)
                               .ToArray();
        var expected = chunks.SelectMany(x => x).ToArray();
        var stream = new SnapshotStream(new InMemorySnapshot(chunks));
        var memory = new MemoryStream();

        stream.CopyTo(memory, bufferSize);
        var actual = memory.ToArray();

        actual.Should()
              .Equal(expected, "содержимое потока должно быть равно содержимому снапшота");
    }
}