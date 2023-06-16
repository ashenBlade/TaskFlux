using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;
using Raft.Core.Commands.RequestVote;

namespace Raft.Network.Tests;

public class SerializationTests
{
    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(1, 2, 3, 4)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, 0, 0, 0)]
    [InlineData(0, 0, 1, 0)]
    [InlineData(1, 0, 1, 0)]
    [InlineData(1, 1234, 45, 90)]
    public void ПриСериализацииRequestVote__ДолженДесериализоватьИдентичныйОбъект(int peerId, int term, int logTerm, int index)
    {
        var requestVote = new RequestVoteRequest(CandidateId: new PeerId(peerId), CandidateTerm: new Term(term),
            LastLog: new LogEntry(new Term(logTerm), index));
        var actual = Serializers.RequestVoteRequest.Deserialize(Serializers.RequestVoteRequest.Serialize(requestVote));
        Assert.Equal(requestVote, actual);
    }
    
    [Theory]
    [InlineData(1, 1, 1, 1, 1)]
    [InlineData(1, 2, 3, 4, 0)]
    [InlineData(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue)]
    [InlineData(0, int.MaxValue, int.MaxValue, int.MaxValue, int.MinValue)]
    [InlineData(0, 1364, 0, -36734, 7754)]
    [InlineData(0, 0, 1, 0, 35624)]
    [InlineData(1, 0, 1, 0, 1)]
    [InlineData(1, 1234, 45, 90, 124)]
    public void ПриСериализацииHeartbeat__ДолженДесериализоватьИдентичныйОбъект(int term, int leaderId, int leaderCommit, int logTerm, int logIndex)
    {
        var heartbeat = new HeartbeatRequest(new Term(term), leaderCommit, new PeerId(leaderId), new LogEntry(new Term(logTerm), logIndex));
        var actual = Serializers.HeartbeatRequest.Deserialize(Serializers.HeartbeatRequest.Serialize(heartbeat));
        Assert.Equal(heartbeat, actual);
    }

    public static IEnumerable<object[]> IntWithBoolPairwise => new[]
    {
        0, 1, -1, 123, int.MaxValue, int.MinValue, 123, 1 << 8, 1 << 8 + 1, 1 << 15, 1 << 31
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