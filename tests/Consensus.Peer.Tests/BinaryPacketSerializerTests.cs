using System.Text;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using TaskFlux.Core;

namespace Consensus.Peer.Tests;

[Trait("Category", "Raft")]
public class BinaryPacketSerializerTests
{
    private static BinaryPacketDeserializer Deserializer { get; } = new();
    private static LogEntry Entry(int term, string data) => new(new Term(term), Encoding.UTF8.GetBytes(data));

    private static void AssertBase(RaftPacket expected)
    {
        var stream = new MemoryStream();
        expected.Serialize(stream, CancellationToken.None);
        stream.Position = 0;
        var actual = Deserializer.Deserialize(stream);
        Assert.Equal(expected, actual, PacketEqualityComparer.Instance);
    }

    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 1, 4)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(87654, 123123, 123, int.MaxValue)]
    [InlineData(345724, 53, 1, 0)]
    [InlineData(2, 3523, 222, 0)]
    [InlineData(1, 23, 20, 0)]
    [InlineData(3, 1234, 45, 90)]
    public void RequestVoteRequest__ДолженДесериализоватьИдентичныйОбъект(
        int peerId,
        int term,
        int logTerm,
        int index)
    {
        var requestVote = new RequestVoteRequest(CandidateId: new NodeId(peerId), CandidateTerm: new Term(term),
            LastLogEntryInfo: new LogEntryInfo(new Term(logTerm), index));
        AssertBase(new RequestVoteRequestPacket(requestVote));
    }

    [Theory]
    [InlineData(1, 1, 1, 1, 1)]
    [InlineData(1, 2, 3, 4, 0)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(1, int.MaxValue, int.MaxValue, int.MaxValue, int.MinValue)]
    [InlineData(321, 1364, 76, 3673, 7754)]
    [InlineData(76, 222222, 1, 333, 35624)]
    [InlineData(134, 987654, 1, 3, 1)]
    [InlineData(98765, 1234, 45, 90, 124)]
    public void AppendEntriesRequest__СПустымМассивомКоманд__ДолженДесериализоватьИдентичныйОбъект(
        int term,
        int leaderId,
        int leaderCommit,
        int logTerm,
        int logIndex)
    {
        var appendEntries = AppendEntriesRequest.Heartbeat(new Term(term), leaderCommit, new NodeId(leaderId),
            new LogEntryInfo(new Term(logTerm), logIndex));
        AssertBase(new AppendEntriesRequestPacket(appendEntries));
    }

    public static IEnumerable<object[]> IntWithBoolPairwise =>
        new[] {1, 123, int.MaxValue, 123, 1 << 8, 1 << 8 + 1, 1 << 15, 1 << 30}.SelectMany(i =>
            new[] {new object[] {i, true}, new object[] {i, false}});

    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public void RequestVoteResponse__ДолженДесериализоватьИдентичныйОбъект(
        int term,
        bool voteGranted)
    {
        var response = new RequestVoteResponse(CurrentTerm: new Term(term), VoteGranted: voteGranted);
        AssertBase(new RequestVoteResponsePacket(response));
    }


    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public void AppendEntriesResponse__ДолженДесериализоватьИдентичныйОбъект(
        int term,
        bool success)
    {
        var response = new AppendEntriesResponse(new Term(term), success);
        AssertBase(new AppendEntriesResponsePacket(response));
    }

    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    public void AppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьОбъектСОднимLogEntry(
        int term,
        int leaderId,
        int leaderCommit,
        int logTerm,
        int logIndex,
        int logEntryTerm,
        string command)
    {
        var appendEntries = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId),
            new LogEntryInfo(new Term(logTerm), logIndex), new[] {Entry(logEntryTerm, command),});
        AssertBase(new AppendEntriesRequestPacket(appendEntries));
    }

    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    [InlineData(50, 12, 40, 30, 30, 4, "вызвать компьютерного мастера")]
    public void AppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьLogEntryСТемиЖеДанными(
        int term,
        int leaderId,
        int leaderCommit,
        int logTerm,
        int logIndex,
        int logEntryTerm,
        string command)
    {
        var expected = Entry(logEntryTerm, command);
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId),
            new LogEntryInfo(new Term(logTerm), logIndex), new[] {expected});
        AssertBase(new AppendEntriesRequestPacket(request));
    }

    public static IEnumerable<object[]> СериализацияAppendEntriesСНесколькимиКомандами = new[]
    {
        new object[] {1, 1, 1, 1, 1, new[] {Entry(1, "payload"), Entry(2, "hello"), Entry(3, "world")}},
        new object[] {2, 1, 1, 5, 2, new[] {Entry(5, ""), Entry(3, "    ")}},
        new object[]
        {
            32, 31, 21, 11, 20,
            new[] {Entry(1, "payload"), Entry(2, "hello"), Entry(3, "world"), Entry(4, "Привет мир")}
        },
    };

    [Theory]
    [MemberData(nameof(СериализацияAppendEntriesСНесколькимиКомандами))]
    public void
        AppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьТакоеЖеКоличествоКоманд(
        int term,
        int leaderId,
        int leaderCommit,
        int logTerm,
        int logIndex,
        LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId),
            new LogEntryInfo(new Term(logTerm), logIndex), entries);
        AssertBase(new AppendEntriesRequestPacket(request));
    }

    [Theory]
    [MemberData(nameof(СериализацияAppendEntriesСНесколькимиКомандами))]
    public void
        AppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьКомандыСТемиЖеСамымиДанными(
        int term,
        int leaderId,
        int leaderCommit,
        int logTerm,
        int logIndex,
        LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId),
            new LogEntryInfo(new Term(logTerm), logIndex), entries);
        AssertBase(new AppendEntriesRequestPacket(request));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(1000)]
    [InlineData(int.MaxValue)]
    [InlineData(87654)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public void ConnectRequest__ДолженДесериализоватьТакуюЖеКоманду(int nodeId)
    {
        AssertBase(new ConnectRequestPacket(new NodeId(nodeId)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConnectResponse__ДолженДесериализоватьТакуюЖеКоманду(bool success)
    {
        AssertBase(new ConnectResponsePacket(success));
    }

    [Theory]
    [InlineData(1, 1, 0, 1)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(333, 2324621, 745, 4365346)]
    [InlineData(63456, int.MaxValue, -1, 4365346)]
    public void InstallSnapshotRequest__ДолженДесериализоватьТакуюЖеКоманду(
        int term,
        int leaderId,
        int lastIndex,
        int lastTerm)
    {
        AssertBase(new InstallSnapshotRequestPacket(new Term(term), new NodeId(leaderId),
            new LogEntryInfo(new Term(lastTerm), lastIndex)));
    }
    // TODO: тесты на точно количество возвращаемых значений

    [Theory]
    [InlineData(new byte[] { })]
    [InlineData(new byte[] {1})]
    [InlineData(new byte[] {0})]
    [InlineData(new byte[] {byte.MaxValue})]
    [InlineData(new byte[] {1, 2})]
    [InlineData(new byte[] {255, 254, 253, 252})]
    [InlineData(new byte[] {1, 1, 2, 44, 128, 88, 33, 2})]
    public void InstallSnapshotChunk__ДолженДесериализоватьТакуюЖеКоманду(byte[] data)
    {
        AssertBase(new InstallSnapshotChunkPacket(data));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(66)]
    [InlineData(123)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(666)]
    public void InstallSnapshotResponse__ДолженДесериализоватьТакуюЖеКоманду(int term)
    {
        AssertBase(new InstallSnapshotResponsePacket(new Term(term)));
    }
}