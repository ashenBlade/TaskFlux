using System.Text;
using Consensus.Core;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Log;
using Consensus.Network;
using Consensus.Network.Packets;
using TaskFlux.Core;

namespace Consensus.Peer.Tests;

[Trait("Category", "Raft")]
public class BinaryPacketSerializerTests
{
    private static BinaryPacketDeserializer Deserializer { get; } = new(); 
    private static LogEntry Entry(int term, string data) => new(new Term(term), Encoding.UTF8.GetBytes(data));

    private static async Task AssertBase(RaftPacket expected)
    {
        var stream = new MemoryStream();
        await expected.Serialize(stream);
        stream.Position = 0;
        var actual = await Deserializer.DeserializeAsync(stream);
        Assert.Equal(expected, actual, PacketEqualityComparer.Instance);
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 1, 4)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, 123123, 123, int.MaxValue)]
    [InlineData(0, 53, 1, 0)]
    [InlineData(0, 3523, 222, 0)]
    [InlineData(1, 23, 20, 0)]
    [InlineData(1, 1234, 45, 90)]
    public async Task ПриСериализацииRequestVoteRequest__ДолженДесериализоватьИдентичныйОбъект(int peerId, int term, int logTerm, int index)
    {
        var requestVote = new RequestVoteRequest(CandidateId: new NodeId(peerId), CandidateTerm: new Term(term),
            LastLogEntryInfo: new LogEntryInfo(new Term(logTerm), index));
        await AssertBase(new RequestVoteRequestPacket(requestVote));
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1)]
    [InlineData(1, 2, 3, 4, 0)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(1, int.MaxValue, int.MaxValue, int.MaxValue, int.MinValue)]
    [InlineData(321, 1364, 76, 3673, 7754)]
    [InlineData(76, 0, 1, 333, 35624)]
    [InlineData(134, 0, 1, 3, 1)]
    [InlineData(98765, 1234, 45, 90, 124)]
    public async Task ПриСериализацииAppendEntriesRequest__СПустымМассивомКоманд__ДолженДесериализоватьИдентичныйОбъект(int term, int leaderId, int leaderCommit, int logTerm, int logIndex)
    {
        var appendEntries = AppendEntriesRequest.Heartbeat(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex));
        await AssertBase(new AppendEntriesRequestPacket(appendEntries));
    }

    public static IEnumerable<object[]> IntWithBoolPairwise => new[]
    {
        1, 123, int.MaxValue, 123, 1 << 8, 1 << 8 + 1, 1 << 15, 1 << 30
    }.SelectMany(i => new[] {new object[] {i, true}, new object[] {i, false}});
    
    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public async Task ПриСериализацииRequestVoteResponse__ДолженДесериализоватьИдентичныйОбъект(int term, bool voteGranted)
    {
        var response = new RequestVoteResponse(CurrentTerm: new Term(term), VoteGranted: voteGranted);
        await AssertBase(new RequestVoteResponsePacket(response));
    }


    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public async Task ПриСериализацииAppendEntriesResponse__ДолженДесериализоватьИдентичныйОбъект(int term, bool success)
    {
        var response = new AppendEntriesResponse(new Term(term), success);
        await AssertBase(new AppendEntriesResponsePacket(response));
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    public async Task ПриСериализацииAppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьОбъектСОднимLogEntry(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, int logEntryTerm, string command)
    {
        var appendEntries = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), new[]
        {
            Entry(logEntryTerm, command),
        });
        await AssertBase(new AppendEntriesRequestPacket(appendEntries));
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    [InlineData(50, 12, 40, 30, 30, 4, "вызвать компьютерного мастера")]
    public async Task ПриСериализацииAppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьLogEntryСТемиЖеДанными(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, int logEntryTerm, string command)
    {
        var expected = Entry(logEntryTerm, command);
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), new[]
        {
            expected
        });
        await AssertBase(new AppendEntriesRequestPacket(request));
    }

    public static IEnumerable<object[]> СериализацияAppendEntriesСНесколькимиКомандами = new[]
    {
        new object[]
        {
            1, 1, 1, 1, 1, new[]{Entry(1, "payload"), Entry(2, "hello"), Entry(3, "world")}
        },
        new object[]
        {
            2, 1, 1, 5, 2, new[]{Entry(5, ""), Entry(3, "    ")}
        },
        new object[]
        {
            32, 31, 21, 11, 20, new[]
            {
                Entry(1, "payload"), Entry(2, "hello"), Entry(3, "world"), Entry(4, "Привет мир")
            }
        },
    };

    [Theory]
    [MemberData(nameof(СериализацияAppendEntriesСНесколькимиКомандами))]
    public async Task ПриСериализацииAppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьТакоеЖеКоличествоКоманд(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), entries);
        await AssertBase(new AppendEntriesRequestPacket(request));
    }
    
    [Theory]
    [MemberData(nameof(СериализацияAppendEntriesСНесколькимиКомандами))]
    public async Task ПриСериализацииAppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьКомандыСТемиЖеСамымиДанными(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), entries);
        await AssertBase(new AppendEntriesRequestPacket(request));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(1000)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    [InlineData(87654)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public async Task ConnectRequest__ДолженДесериализоватьТакуюЖеКоманду(int nodeId)
    {
        await AssertBase(new ConnectRequestPacket(new NodeId(nodeId)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConnectResponse__ДолженДесериализоватьТакуюЖеКоманду(bool success)
    {
        await AssertBase(new ConnectResponsePacket(success));
    }
}