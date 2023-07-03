using System.Text;
using Raft.Core;
using Raft.Core.Commands.AppendEntries;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;

namespace Raft.Peer.Tests;

public class SerializationTests
{
    private static LogEntry Entry(int term, string data) => new LogEntry(new Term(term), Encoding.UTF8.GetBytes(data));
    
    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 1, 4)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, 123123, 123, int.MaxValue)]
    [InlineData(0, 53, 1, 0)]
    [InlineData(0, 3523, 222, 0)]
    [InlineData(1, 23, 20, 0)]
    [InlineData(1, 1234, 45, 90)]
    public void ПриСериализацииRequestVoteRequest__ДолженДесериализоватьИдентичныйОбъект(int peerId, int term, int logTerm, int index)
    {
        var requestVote = new RequestVoteRequest(CandidateId: new NodeId(peerId), CandidateTerm: new Term(term),
            LastLogEntryInfo: new LogEntryInfo(new Term(logTerm), index));
        var actual = Serializers.RequestVoteRequest.Deserialize(Serializers.RequestVoteRequest.Serialize(requestVote));
        Assert.Equal(requestVote, actual);
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
    public void ПриСериализацииAppendEntriesRequest__СПустымМассивомКоманд__ДолженДесериализоватьИдентичныйОбъект(int term, int leaderId, int leaderCommit, int logTerm, int logIndex)
    {
        var heartbeat = AppendEntriesRequest.Heartbeat(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex));
        var actual = Serializers.AppendEntriesRequest.Deserialize(Serializers.AppendEntriesRequest.Serialize(heartbeat));
        Assert.Equal(heartbeat, actual);
    }

    public static IEnumerable<object[]> IntWithBoolPairwise => new[]
    {
        1, 123, int.MaxValue, 123, 1 << 8, 1 << 8 + 1, 1 << 15, 1 << 30
    }.SelectMany(i => new[] {new object[] {i, true}, new object[] {i, false}});
    
    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public void ПриСериализацииRequestVoteResponse__ДолженДесериализоватьИдентичныйОбъект(int term, bool voteGranted)
    {
        var response = new RequestVoteResponse(CurrentTerm: new Term(term), VoteGranted: voteGranted);
        var actual = Serializers.RequestVoteResponse.Deserialize(Serializers.RequestVoteResponse.Serialize(response));
        Assert.Equal(response, actual);
    }


    [Theory]
    [MemberData(nameof(IntWithBoolPairwise))]
    public void ПриСериализацииAppendEntriesResponse__ДолженДесериализоватьИдентичныйОбъект(int term, bool success)
    {
        var response = new AppendEntriesResponse(new Term(term), success);
        var actual = Serializers.AppendEntriesResponse.Deserialize(Serializers.AppendEntriesResponse.Serialize(response));
        Assert.Equal(response, actual);
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    public void ПриСериализацииAppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьОбъектСОднимLogEntry(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, int logEntryTerm, string command)
    {
        var heartbeat = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), new[]
        {
            Entry(logEntryTerm, command),
        });
        var actual = Serializers.AppendEntriesRequest.Deserialize(Serializers.AppendEntriesRequest.Serialize(heartbeat));
        Assert.Single(actual.Entries.ToArray());
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1, 1, "hello")]
    [InlineData(3, 2, 22, 3, 2, 2, "")]
    [InlineData(3, 2, 22, 3, 2, 3, "                 ")]
    [InlineData(50, 2, 30, 3, 30, 2, "\n\n")]
    [InlineData(50, 12, 40, 30, 30, 4, "вызвать компьютерного мастера")]
    public void ПриСериализацииAppendEntriesRequest__СОднойКомандой__ДолженДесериализоватьLogEntryСТемиЖеДанными(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, int logEntryTerm, string command)
    {
        var expected = Entry(logEntryTerm, command);
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), new[]
        {
            expected
        });
        var actual = Serializers.AppendEntriesRequest.Deserialize(Serializers.AppendEntriesRequest.Serialize(request)).Entries.Single();
        Assert.Equal(expected, actual, LogEntryEqualityComparer.Instance);
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
    public void ПриСериализацииAppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьТакоеЖеКоличествоКоманд(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), entries);
        var actual = Serializers.AppendEntriesRequest.Deserialize(Serializers.AppendEntriesRequest.Serialize(request)).Entries;
        Assert.Equal(entries.Length, actual.Count);
    }
    
    [Theory]
    [MemberData(nameof(СериализацияAppendEntriesСНесколькимиКомандами))]
    public void ПриСериализацииAppendEntriesRequest__СНесколькимиКомандами__ДолженДесериализоватьКомандыСТемиЖеСамымиДанными(
        int term, int leaderId, int leaderCommit, int logTerm, int logIndex, LogEntry[] entries)
    {
        var request = new AppendEntriesRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntryInfo(new Term(logTerm), logIndex), entries);
        var actual = Serializers.AppendEntriesRequest.Deserialize(Serializers.AppendEntriesRequest.Serialize(request)).Entries;
        Assert.Equal(entries, actual, LogEntryEqualityComparer.Instance);
    }
}