using Raft.Core;
using Raft.Core.Log;

namespace Raft.Network;

public static class Serializers
{
    public static class RequestVoteRequest
    {
        public static byte[] Serialize(Core.Commands.RequestVote.RequestVoteRequest request)
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

        public static Core.Commands.RequestVote.RequestVoteRequest Deserialize(byte[] buffer)
        {
            var stream = new MemoryStream(buffer);
            using var reader = new BinaryReader(stream);
            var marker = reader.ReadByte();
            if (marker is not (byte)RequestType.RequestVote)
            {
                throw new ArgumentException($"Первый байт должен быть равен {(byte)RequestType.RequestVote}: передано {marker}");
            }

            var candidateId = reader.ReadInt32();
            var candidateTerm = reader.ReadInt32();
            var lastLogIndex = reader.ReadInt32();
            var lastLogTerm = reader.ReadInt32();
        
            return new Core.Commands.RequestVote.RequestVoteRequest(CandidateId: new(candidateId), CandidateTerm: new(candidateTerm),
                LastLog: new LogEntryInfo(new(lastLogTerm), lastLogIndex));
        }
    }

    public static class RequestVoteResponse
    {
        public static byte[] Serialize(Core.Commands.RequestVote.RequestVoteResponse response)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)RequestType.RequestVote);
            writer.Write(response.VoteGranted);
            writer.Write(response.CurrentTerm.Value);
        
            return stream.ToArray();
        }

        public static Core.Commands.RequestVote.RequestVoteResponse Deserialize(byte[] buffer)
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
        
            return new Core.Commands.RequestVote.RequestVoteResponse(CurrentTerm: new Term(currentTerm), VoteGranted: voteGranted);
        }
    }

    public static class AppendEntriesRequest
    {
        public static byte[] Serialize(Core.Commands.AppendEntries.AppendEntriesRequest request)
        {
            // Маркерный байт + 4 значения по 4 байта
            const int initialBufferSize = 1 + 4 * 5;
            var stream = new MemoryStream(initialBufferSize);
            var writer = new BinaryWriter(stream);
        
            writer.Write((byte)RequestType.AppendEntries);
            writer.Write(request.LeaderId.Value);
            writer.Write(request.Term.Value);
            writer.Write(request.LeaderCommit);
            writer.Write(request.PrevLogEntryInfo.Term.Value);
            writer.Write(request.PrevLogEntryInfo.Index);

            return stream.ToArray();
        }
        
        public static Core.Commands.AppendEntries.AppendEntriesRequest Deserialize(byte[] buffer)
        {
            using var reader = new BinaryReader(new MemoryStream(buffer));
            var marker = reader.ReadByte();
            if (marker is not (byte)RequestType.AppendEntries)
            {
                throw new ArgumentException();
            }

            var leaderId = reader.ReadInt32();
            var term = reader.ReadInt32();
            var commit = reader.ReadInt32();
            var prevLogEntryTerm = reader.ReadInt32();
            var prevLogEntryIndex = reader.ReadInt32();

            return new Core.Commands.AppendEntries.AppendEntriesRequest(LeaderId: new(leaderId), Term: new(term), LeaderCommit: commit,
                PrevLogEntryInfo: new LogEntryInfo(new(prevLogEntryTerm), prevLogEntryIndex), Entries: Array.Empty<LogEntry>());
        }
    }

    public static class AppendEntriesResponse
    {
        public static byte[] Serialize(Core.Commands.AppendEntries.AppendEntriesResponse response)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)RequestType.AppendEntries);
            writer.Write(response.Success);
            writer.Write(response.Term.Value);
            return stream.ToArray();
        }
        
        public static Core.Commands.AppendEntries.AppendEntriesResponse Deserialize(byte[] buffer)
        {
            using var reader = new BinaryReader(new MemoryStream(buffer));
            var marker = reader.ReadByte();
            if (marker is not (byte) RequestType.AppendEntries)
            {
                throw new ArgumentException($"Первый байт не AppendEntries. Получено: {marker}");
            }
            var success = reader.ReadBoolean();
            var term = reader.ReadInt32();
            
            return new Core.Commands.AppendEntries.AppendEntriesResponse(new Term(term), success);
        }
    }
}