using FluentAssertions;
using Moq;

namespace Consensus.Raft.Tests;

[Trait("Category", "Raft")]
public class PeerGroupTests
{
    [Fact]
    public void IsQuorumReached__КогдаУзелОдинИГолосОдин__ДолженВернутьTrue()
    {
        var group = new PeerGroup(CreateNodes(1));

        var granted = group.IsQuorumReached(1);

        granted.Should().BeTrue("один из группы из одного - это все");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void IsQuorumReached__Голосов0__ДолженВернутьFalse(int nodesCount)
    {
        var group = new PeerGroup(CreateNodes(nodesCount));

        var granted = group.IsQuorumReached(0);

        granted.Should()
               .BeFalse("никто не проголосовал");
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(3, 3)]
    [InlineData(4, 2)]
    [InlineData(5, 3)]
    [InlineData(5, 4)]
    public void IsQuorumReached__КогдаУзловНесколькоИГолосовБольшеПоловины__ДолженВернутьTrue(int peersCount, int votes)
    {
        var group = new PeerGroup(CreateNodes(peersCount));

        var granted = group.IsQuorumReached(votes);

        granted.Should()
               .BeTrue("включая текущий узел - проголосовало большинство (узлов: {0}, проголосовало: {1})",
                    peersCount + 1, votes);
    }

    [Theory]
    [InlineData(3, 1)]
    [InlineData(4, 1)]
    [InlineData(5, 1)]
    [InlineData(5, 2)]
    [InlineData(6, 1)]
    [InlineData(6, 2)]
    public void IsQuorumReached__КогдаУзловНесколькоИГолосовНеБольшеПоловины__ДолженВернутьFalse(
        int peersCount,
        int votes)
    {
        var group = new PeerGroup(CreateNodes(peersCount));

        var granted = group.IsQuorumReached(votes);

        granted.Should()
               .BeFalse("включая текущий узел - проголосовало меньшинство (узлов: {0}, проголосовало: {1})",
                    peersCount + 1, votes);
    }

    private static readonly IPeer Peer = Mock.Of<IPeer>();

    private static IPeer[] CreateNodes(int count)
    {
        var result = new IPeer[count];
        Array.Fill(result, Peer);
        return result;
    }

    [Fact]
    public void IsQuorumReached__КогдаУзловНет__ДолженВернутьTrueПри0()
    {
        var group = new PeerGroup(Array.Empty<IPeer>());

        var actual = group.IsQuorumReached(0);

        actual
           .Should()
           .BeTrue("других узлов нет, а сами с собой кворума достигаем");
    }
}