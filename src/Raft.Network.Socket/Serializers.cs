using System.Runtime.CompilerServices;
using System.Text;
using Raft.Core;
using Raft.Core.Log;
using Raft.Network.Packets;

[assembly: InternalsVisibleTo("Raft.Network.Socket.Tests")]
namespace Raft.Network.Socket;

internal static class Serializers
{
    public static class RequestVoteRequest
    {
        public static byte[] Serialize(Core.Commands.RequestVote.RequestVoteRequest request)
        {
            // Маркерный байт + 4 значения по 4 байта
            const int initialBufferSize = 1 + 4 * 4;
            var stream = new MemoryStream(initialBufferSize);
            using var writer = new BinaryWriter(stream);
        
            writer.Write((byte)PacketType.RequestVoteRequest);
            writer.Write(request.CandidateId.Value);
            writer.Write(request.CandidateTerm.Value);
            writer.Write(request.LastLogEntryInfo.Index);
            writer.Write(request.LastLogEntryInfo.Term.Value);
        
            return stream.ToArray();
        }

        public static Core.Commands.RequestVote.RequestVoteRequest Deserialize(byte[] buffer)
        {
            var stream = new MemoryStream(buffer);
            using var reader = new BinaryReader(stream);
            var marker = reader.ReadByte();
            if (marker is not (byte)PacketType.RequestVoteRequest)
            {
                throw new ArgumentException($"Первый байт должен быть равен {(byte)PacketType.RequestVoteRequest}: передано {marker}");
            }

            var candidateId = reader.ReadInt32();
            var candidateTerm = reader.ReadInt32();
            var lastLogIndex = reader.ReadInt32();
            var lastLogTerm = reader.ReadInt32();
        
            return new Core.Commands.RequestVote.RequestVoteRequest(CandidateId: new(candidateId), CandidateTerm: new(candidateTerm),
                LastLogEntryInfo: new LogEntryInfo(new(lastLogTerm), lastLogIndex));
        }
    }

    public static class RequestVoteResponse
    {
        public static byte[] Serialize(Core.Commands.RequestVote.RequestVoteResponse response)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)PacketType.RequestVoteResponse);
            writer.Write(response.VoteGranted);
            writer.Write(response.CurrentTerm.Value);
        
            return stream.ToArray();
        }

        public static Core.Commands.RequestVote.RequestVoteResponse Deserialize(byte[] buffer)
        {
            var stream = new MemoryStream(buffer);
            using var reader = new BinaryReader(stream);
            var marker = reader.ReadByte();
            if (marker is not (byte)PacketType.RequestVoteResponse)
            {
                throw new ArgumentException($"Первый байт должен быть равен {(byte)PacketType.RequestVoteRequest}: передано {marker}");
            }

            var voteGranted = reader.ReadBoolean();
            var currentTerm = reader.ReadInt32();
        
            return new Core.Commands.RequestVote.RequestVoteResponse(CurrentTerm: new Term(currentTerm), VoteGranted: voteGranted);
        }
    }

    public static class AppendEntriesRequest
    {
        private static readonly Encoding Encoding = Encoding.UTF8;

        public static byte[] Serialize(Core.Commands.AppendEntries.AppendEntriesRequest request)
        {
            // Маркерный байт + 4 значения по 4 байта
            const int initialBufferSize = 1 + 4 * 5;
            var stream = new MemoryStream(initialBufferSize);
            var writer = new BinaryWriter(stream, Encoding);
            
            writer.Write((byte)PacketType.AppendEntriesRequest);
            writer.Write(request.LeaderId.Value);
            writer.Write(request.Term.Value);
            writer.Write(request.LeaderCommit);
            writer.Write(request.PrevLogEntryInfo.Term.Value);
            writer.Write(request.PrevLogEntryInfo.Index);

            writer.Write(request.Entries.Count);
            
            foreach (var entry in request.Entries)
            {
                writer.Write(entry.Term.Value);
                writer.Write(entry.Data);
            }

            return stream.ToArray();
        }
        
        public static Core.Commands.AppendEntries.AppendEntriesRequest Deserialize(byte[] buffer)
        {
            using var reader = new BinaryReader(new MemoryStream(buffer), Encoding);
            var marker = reader.ReadByte();
            if (marker is not (byte)PacketType.AppendEntriesRequest)
            {
                throw new ArgumentException();
            }

            var leaderId = reader.ReadInt32();
            var term = reader.ReadInt32();
            var commit = reader.ReadInt32();
            var prevLogEntryTerm = reader.ReadInt32();
            var prevLogEntryIndex = reader.ReadInt32();

            var entriesCount = reader.ReadInt32();
            if (entriesCount == 0)
            {
                return new Core.Commands.AppendEntries.AppendEntriesRequest(LeaderId: new(leaderId), Term: new(term), LeaderCommit: commit,
                    PrevLogEntryInfo: new LogEntryInfo(new(prevLogEntryTerm), prevLogEntryIndex), Entries: Array.Empty<LogEntry>());
            }
            
            var entries = new List<LogEntry>();

            for (int i = 0; i < entriesCount; i++)
            {
                var logTerm = reader.ReadInt32();
                var data = reader.ReadString();
                entries.Add(new LogEntry(new Term(logTerm), data));
            }
            
            return new Core.Commands.AppendEntries.AppendEntriesRequest(LeaderId: new(leaderId), Term: new(term), LeaderCommit: commit,
                PrevLogEntryInfo: new LogEntryInfo(new(prevLogEntryTerm), prevLogEntryIndex), Entries: entries);
        }
    }

    public static class AppendEntriesResponse
    {
        public static byte[] Serialize(Core.Commands.AppendEntries.AppendEntriesResponse response)
        {
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)PacketType.AppendEntriesResponse);
            writer.Write(response.Success);
            writer.Write(response.Term.Value);
            return stream.ToArray();
        }
        
        public static Core.Commands.AppendEntries.AppendEntriesResponse Deserialize(byte[] buffer)
        {
            using var reader = new BinaryReader(new MemoryStream(buffer));
            var marker = reader.ReadByte();
            if (marker is not (byte) PacketType.AppendEntriesResponse)
            {
                throw new ArgumentException($"Первый байт не AppendEntries. Получено: {marker}");
            }
            var success = reader.ReadBoolean();
            var term = reader.ReadInt32();
            
            return new Core.Commands.AppendEntries.AppendEntriesResponse(new Term(term), success);
        }
    }

    public static class ConnectRequest
    {
        public static byte[] Serialize(ConnectRequestPacket packet)
        {
            using var stream = new MemoryStream(sizeof(byte) + sizeof(int));
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)PacketType.ConnectRequest);
            writer.Write(packet.Id.Value);
            return stream.ToArray();
        }

        public static ConnectRequestPacket Deserialize(byte[] buffer)
        {
            using var stream = new MemoryStream(buffer);
            using var reader = new BinaryReader(stream);

            var marker = (PacketType) reader.ReadByte();
            if (marker is not PacketType.ConnectRequest)
            {
                throw new ArgumentException($"Ожидался ConnectRequest маркер. Получен: {marker}");
            }

            var id = reader.ReadInt32();
            return new ConnectRequestPacket(new NodeId(id));
        }
    }

    public static class ConnectResponse
    {
        public static byte[] Serialize(ConnectResponsePacket packet)
        {
            using var stream = new MemoryStream(sizeof(byte) + sizeof(byte));
            using var writer = new BinaryWriter(stream);

            writer.Write((byte)PacketType.ConnectResponse);
            writer.Write(packet.Success);
            return stream.ToArray();
        }

        public static ConnectResponsePacket Deserialize(byte[] buffer)
        {
            using var stream = new MemoryStream(buffer);
            using var reader = new BinaryReader(stream);

            var marker = (PacketType) reader.ReadByte();
            if (marker is not PacketType.ConnectResponse)
            {
                throw new ArgumentException($"Ожидался ConnectResponse маркер. Получен: {marker}");
            }

            var success = reader.ReadBoolean();
            return new ConnectResponsePacket(success);
        }
    }
}