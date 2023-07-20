using System.ComponentModel;
using Consensus.Core;
using Consensus.Core.Commands.AppendEntries;
using Consensus.Core.Commands.RequestVote;
using Consensus.Core.Log;
using Consensus.Network;
using Consensus.Network.Packets;

namespace Consensus.Peer;

public class BinaryPacketSerializer: IPacketSerializer
{
    public static readonly BinaryPacketSerializer Instance = new();
    public void Serialize(IPacket packet, BinaryWriter writer)
    {
        switch (packet.PacketType)
        {
            case PacketType.ConnectRequest:
                Serialize(packet.As<ConnectRequestPacket>(), writer);
                break;
            case PacketType.ConnectResponse:
                Serialize(packet.As<ConnectResponsePacket>(), writer);
                break;
            case PacketType.AppendEntriesRequest:
                Serialize(packet.As<AppendEntriesRequestPacket>(), writer);
                break;
            case PacketType.AppendEntriesResponse:
                Serialize(packet.As<AppendEntriesResponsePacket>(), writer);
                break;
            case PacketType.RequestVoteRequest:
                Serialize(packet.As<RequestVoteRequestPacket>(), writer);
                break;
            case PacketType.RequestVoteResponse:
                Serialize(packet.As<RequestVoteResponsePacket>(), writer);
                break;
            default:
                throw new InvalidEnumArgumentException(nameof(packet.PacketType), ( byte ) packet.PacketType,
                    typeof(PacketType));
        }
    }

    public IPacket Deserialize(BinaryReader reader)
    {
        var packetType = (PacketType) reader.ReadByte();
        switch (packetType)
        {
            case PacketType.ConnectRequest:
                return DeserializeConnectRequestPacket(reader);
            case PacketType.ConnectResponse:
                return DeserializeConnectResponsePacket(reader);
            case PacketType.RequestVoteRequest:
                return DeserializeRequestVoteRequestPacket(reader);
            case PacketType.RequestVoteResponse:
                return DeserializeRequestVoteResponsePacket(reader);
            case PacketType.AppendEntriesRequest:
                return DeserializeAppendEntriesRequestPacket(reader);
            case PacketType.AppendEntriesResponse:
                return DeserializeAppendEntriesResponsePacket(reader);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private void Serialize(AppendEntriesResponsePacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.AppendEntriesResponse);
        writer.Write(packet.EstimatePacketSize());
        writer.Write(packet.Response.Success);
        writer.Write(packet.Response.Term.Value);
    }
    
    private AppendEntriesResponsePacket DeserializeAppendEntriesResponsePacket(BinaryReader reader)
    {
        _ = reader.ReadInt32();

        var success = reader.ReadBoolean();
        var term = reader.ReadInt32();
        
        return new AppendEntriesResponsePacket(new AppendEntriesResponse(new Term(term), success));
    }

    private void Serialize(AppendEntriesRequestPacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.AppendEntriesRequest);
        writer.Write(packet.EstimatePacketSize());
        var request = packet.Request;
        writer.Write(request.Term.Value);
        writer.Write(request.LeaderId.Value);
        writer.Write(request.LeaderCommit);
        writer.Write(request.PrevLogEntryInfo.Term.Value);
        writer.Write(request.PrevLogEntryInfo.Index);
        writer.Write(request.Entries.Count);
        if (request.Entries.Count == 0)
        {
            return;
        }
        
        foreach (var entry in request.Entries)
        {
            writer.Write(entry.Term.Value);
            writer.Write(entry.Data.Length);
            writer.Write(entry.Data);
        }
    }

    private AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(BinaryReader reader)
    {
        _ = reader.ReadInt32();

        var term = reader.ReadInt32();
        var leaderId = reader.ReadInt32();
        var leaderCommit = reader.ReadInt32();
        var entryTerm = reader.ReadInt32();
        var entryIndex = reader.ReadInt32();
        var entriesCount = reader.ReadInt32();
        IReadOnlyList<LogEntry> entries;
        if (entriesCount == 0)
        {
            entries = Array.Empty<LogEntry>();
        }
        else
        {
            var list = new List<LogEntry>();
            for (int i = 0; i < entriesCount; i++)
            {
                var logEntryTerm = reader.ReadInt32();                
                var payloadLength = reader.ReadInt32();
                var payload = new byte[payloadLength];
                var read = reader.Read(payload);
                if (read != payloadLength)
                {
                    throw new InvalidDataException(
                        $"Прочитанный размер буфера не равен указанному размеру. Указано: {payloadLength}. Прочитано: {read}");
                }
                list.Add(new LogEntry(new Term(logEntryTerm), payload));
            }

            entries = list;
        }
        return new AppendEntriesRequestPacket(new AppendEntriesRequest(new Term(term), leaderCommit,
            new NodeId(leaderId), new LogEntryInfo(new Term(entryTerm), entryIndex), entries));
    }

    private RequestVoteResponsePacket DeserializeRequestVoteResponsePacket(BinaryReader reader)
    {
        _ = reader.ReadInt32();

        var success = reader.ReadBoolean();
        var term = reader.ReadInt32();

        return new RequestVoteResponsePacket(new RequestVoteResponse(new Term(term), success));
    }

    private void Serialize(RequestVoteResponsePacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.RequestVoteResponse);
        writer.Write(packet.EstimatePacketSize());
        writer.Write(packet.Response.VoteGranted);
        writer.Write(packet.Response.CurrentTerm.Value);
    }

    private RequestVoteRequestPacket DeserializeRequestVoteRequestPacket(BinaryReader reader)
    {
        _ = reader.ReadInt32();
        var id = reader.ReadInt32();
        var term = reader.ReadInt32();
        var entryTerm = reader.ReadInt32();
        var entryIndex = reader.ReadInt32();
        return new RequestVoteRequestPacket(new RequestVoteRequest(new NodeId(id), new Term(term),
            new LogEntryInfo(new Term(entryTerm), entryIndex)));
    }

    private void Serialize(RequestVoteRequestPacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.RequestVoteRequest);
        writer.Write(packet.EstimatePacketSize());
        writer.Write(packet.Request.CandidateId.Value);
        writer.Write(packet.Request.CandidateTerm.Value);
        writer.Write(packet.Request.LastLogEntryInfo.Term.Value);
        writer.Write(packet.Request.LastLogEntryInfo.Index);
    }

    private ConnectResponsePacket DeserializeConnectResponsePacket(BinaryReader reader)
    {
        _ = reader.ReadInt32();
        var success = reader.ReadBoolean();
        return new ConnectResponsePacket(success);
    }
    
    private void Serialize(ConnectResponsePacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.ConnectResponse);
        writer.Write(packet.EstimatePacketSize());
        writer.Write(packet.Success);
    }

    private ConnectRequestPacket DeserializeConnectRequestPacket(BinaryReader reader)
    {
        _ = reader.ReadInt32(); // Длина
        var nodeId = reader.ReadInt32();
        return new ConnectRequestPacket(new NodeId(nodeId));
    }

    private void Serialize(ConnectRequestPacket packet, BinaryWriter writer)
    {
        writer.Write((byte)PacketType.ConnectRequest);
        writer.Write(packet.EstimatePacketSize());
        writer.Write(packet.Id.Value);
    }
}