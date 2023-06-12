using Raft.Core;
using Raft.Core.Commands;
using Raft.Core.Commands.Heartbeat;

namespace Raft.Peer;

public static class Helpers
{
    public static byte[] Serialize(RequestVoteRequest request)
    {
        // Маркерный байт + 4 значения по 4 байта
        const int initialBufferSize = 1 + 4 * 4;
        var stream = new MemoryStream(initialBufferSize);
        using var writer = new BinaryWriter(stream);
        
        writer.Write((byte)RequestType.RequestVote);
        writer.Write(request.CandidateId.Value);
        writer.Write(request.CandidateTerm.Value);
        writer.Write(request.LastLog.Index);
        writer.Write(request.LastLog.Term.Value);
        
        return stream.ToArray();
    }

    public static RequestVoteResponse DeserializeRequestVoteResponse(byte[] buffer)
    {
        var stream = new MemoryStream(buffer);
        using var reader = new BinaryReader(stream);
        var marker = reader.ReadByte();
        if (marker is not (byte)RequestType.RequestVote)
        {
            throw new ArgumentException($"Первый байт должен быть равен {(byte)RequestType.RequestVote}: передано {marker}");
        }

        var voteGranted = reader.ReadBoolean();
        var currentTerm = reader.ReadInt32();
        
        return new RequestVoteResponse()
        {
            CurrentTerm = new Term(currentTerm),
            VoteGranted = voteGranted
        };
    }

    public static byte[] Serialize(HeartbeatRequest request)
    {
        // Маркерный байт + 4 значения по 4 байта
        const int initialBufferSize = 1 + 4 * 5;
        var stream = new MemoryStream(initialBufferSize);
        var writer = new BinaryWriter(stream);
        
        writer.Write((byte)RequestType.AppendEntries);
        writer.Write(request.LeaderId.Value);
        writer.Write(request.Term.Value);
        writer.Write(request.LeaderCommit);
        writer.Write(request.PrevLogEntry.Term.Value);
        writer.Write(request.PrevLogEntry.Index);

        return stream.ToArray();
        
    }

    public static HeartbeatResponse DeserializeHeartbeatResponse(byte[] buffer)
    {
        using var reader = new BinaryReader(new MemoryStream(buffer));
        var marker = reader.ReadByte();
        if (marker is not (byte) RequestType.AppendEntries)
        {
            throw new ArgumentException($"Первый байт не AppendEntries. Получено: {marker}");
        }

        var term = reader.ReadInt32();
        var success = reader.ReadBoolean();
        return new HeartbeatResponse(new Term(term), success);
    }
}