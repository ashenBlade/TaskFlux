using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;
using Raft.Core.Log;

namespace Raft.Network.Tests;

public class SerializationTests
{
    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 1, 4)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, 123123, 123, int.MaxValue)]
    [InlineData(0, 53, 1, 0)]
    [InlineData(0, 3523, 222, 0)]
    [InlineData(1, 23, 20, 0)]
    [InlineData(1, 1234, 45, 90)]
    public void ПриСериализацииRequestVote__ДолженДесериализоватьИдентичныйОбъект(int peerId, int term, int logTerm, int index)
    {
        var requestVote = new RequestVoteRequest(CandidateId: new NodeId(peerId), CandidateTerm: new Term(term),
            LastLog: new LogEntry(new Term(logTerm), index));
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
    public void ПриСериализацииHeartbeat__ДолженДесериализоватьИдентичныйОбъект(int term, int leaderId, int leaderCommit, int logTerm, int logIndex)
    {
        var heartbeat = new HeartbeatRequest(new Term(term), leaderCommit, new NodeId(leaderId), new LogEntry(new Term(logTerm), logIndex));
        var actual = Serializers.HeartbeatRequest.Deserialize(Serializers.HeartbeatRequest.Serialize(heartbeat));
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
    public void ПриСериализацииHeartbeatResponse__ДолженДесериализоватьИдентичныйОбъект(int term, bool success)
    {
        var response = new HeartbeatResponse(new Term(term), success);
        var actual = Serializers.HeartbeatResponse.Deserialize(Serializers.HeartbeatResponse.Serialize(response));
        Assert.Equal(response, actual);
    }
}