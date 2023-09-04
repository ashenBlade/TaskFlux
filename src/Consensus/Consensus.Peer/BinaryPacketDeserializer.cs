using System.Buffers;
using System.Net.Sockets;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using TaskFlux.Core;
using TaskFlux.Serialization.Helpers;

namespace Consensus.Peer;

public class BinaryPacketDeserializer
{
    public static readonly BinaryPacketDeserializer Instance = new();

    public RaftPacket Deserialize(Stream stream, CancellationToken token = default)
    {
        Span<byte> array = stackalloc byte[1];
        var read = stream.Read(array);
        if (read == 0)
        {
            throw new SocketException(( int ) SocketError.Shutdown);
        }

        var packetType = ( RaftPacketType ) array[0];
        return packetType switch
               {
                   RaftPacketType.ConnectRequest          => DeserializeConnectRequestPacket(stream, token),
                   RaftPacketType.ConnectResponse         => DeserializeConnectResponsePacket(stream, token),
                   RaftPacketType.RequestVoteRequest      => DeserializeRequestVoteRequestPacket(stream, token),
                   RaftPacketType.RequestVoteResponse     => DeserializeRequestVoteResponsePacket(stream, token),
                   RaftPacketType.AppendEntriesRequest    => DeserializeAppendEntriesRequestPacket(stream, token),
                   RaftPacketType.AppendEntriesResponse   => DeserializeAppendEntriesResponsePacket(stream, token),
                   RaftPacketType.InstallSnapshotRequest  => DeserializeInstallSnapshotRequestPacket(stream, token),
                   RaftPacketType.InstallSnapshotChunk    => DeserializeInstallSnapshotChunkPacket(stream, token),
                   RaftPacketType.InstallSnapshotResponse => DeserializeInstallSnapshotResponsePacket(stream, token),
               };
    }

    private InstallSnapshotResponsePacket DeserializeInstallSnapshotResponsePacket(
        Stream stream,
        CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);

        var term = reader.ReadInt32();

        return new InstallSnapshotResponsePacket(new Term(term));
    }

    private InstallSnapshotChunkPacket DeserializeInstallSnapshotChunkPacket(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var buffer = reader.ReadBuffer();
        return new InstallSnapshotChunkPacket(buffer);
    }

    private InstallSnapshotRequestPacket DeserializeInstallSnapshotRequestPacket(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);

        var term = reader.ReadInt32();
        var leaderId = reader.ReadInt32();
        var lastIndex = reader.ReadInt32();
        var lastTerm = reader.ReadInt32();

        return new InstallSnapshotRequestPacket(new Term(term), new NodeId(leaderId),
            new LogEntryInfo(new Term(lastTerm), lastIndex));
    }

    private AppendEntriesResponsePacket DeserializeAppendEntriesResponsePacket(Stream stream, CancellationToken token)
    {
        return DeserializeAppendEntriesResponsePacketAsync(stream, token).GetAwaiter().GetResult();
    }

    private AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(Stream stream, CancellationToken token)
    {
        return DeserializeAppendEntriesRequestPacketAsync(stream, token).GetAwaiter().GetResult();
    }

    private RequestVoteResponsePacket DeserializeRequestVoteResponsePacket(Stream stream, CancellationToken token)
    {
        return DeserializeRequestVoteResponsePacketAsync(stream, token).GetAwaiter().GetResult();
    }

    private RequestVoteRequestPacket DeserializeRequestVoteRequestPacket(Stream stream, CancellationToken token)
    {
        return DeserializeRequestVoteRequestPacketAsync(stream, token).GetAwaiter().GetResult();
    }

    private ConnectResponsePacket DeserializeConnectResponsePacket(Stream stream, CancellationToken token)
    {
        return DeserializeConnectResponsePacketAsync(stream, token).GetAwaiter().GetResult();
    }

    public async ValueTask<RaftPacket> DeserializeAsync(Stream stream, CancellationToken token = default)
    {
        var array = ArrayPool<byte>.Shared.Rent(1);
        RaftPacketType packetType;
        try
        {
            var read = await stream.ReadAsync(array.AsMemory(0, 1), token);
            if (read == 0)
            {
                throw new SocketException(( int ) SocketError.Shutdown);
            }

            packetType = ( RaftPacketType ) array[0];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(array);
        }

        return packetType switch
               {
                   RaftPacketType.ConnectRequest      => await DeserializeConnectRequestPacketAsync(stream, token),
                   RaftPacketType.ConnectResponse     => await DeserializeConnectResponsePacketAsync(stream, token),
                   RaftPacketType.RequestVoteRequest  => await DeserializeRequestVoteRequestPacketAsync(stream, token),
                   RaftPacketType.RequestVoteResponse => await DeserializeRequestVoteResponsePacketAsync(stream, token),
                   RaftPacketType.AppendEntriesRequest => await DeserializeAppendEntriesRequestPacketAsync(stream,
                                                              token),
                   RaftPacketType.AppendEntriesResponse => await DeserializeAppendEntriesResponsePacketAsync(stream,
                                                               token),
                   _ => throw new ArgumentOutOfRangeException()
               };
    }

    private static async ValueTask<AppendEntriesResponsePacket> DeserializeAppendEntriesResponsePacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var buffer = await ReadRequiredLengthAsync(stream, sizeof(bool) + sizeof(int), token);
        try
        {
            var reader = new ArrayBinaryReader(buffer);
            var success = reader.ReadBoolean();
            var term = reader.ReadInt32();

            return new AppendEntriesResponsePacket(new AppendEntriesResponse(new Term(term), success));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<AppendEntriesRequestPacket> DeserializeAppendEntriesRequestPacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var buffer = await ReadRequiredLengthAsync(stream, sizeof(int), token);
        int totalPacketLength;
        try
        {
            var reader = new ArrayBinaryReader(buffer);
            totalPacketLength = reader.ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        buffer = await ReadRequiredLengthAsync(stream, totalPacketLength, token);
        try
        {
            var reader = new ArrayBinaryReader(buffer);

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
                    var payload = reader.ReadBuffer();
                    list.Add(new LogEntry(new Term(logEntryTerm), payload));
                }

                entries = list;
            }

            return new AppendEntriesRequestPacket(new AppendEntriesRequest(new Term(term), leaderCommit,
                new NodeId(leaderId), new LogEntryInfo(new Term(entryTerm), entryIndex), entries));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<RequestVoteResponsePacket> DeserializeRequestVoteResponsePacketAsync(
        Stream stream,
        CancellationToken token)
    {
        const int packetSize = sizeof(bool) // Success 
                             + sizeof(int); // Term
        var buffer = await ReadRequiredLengthAsync(stream, packetSize, token);
        try
        {
            var reader = new ArrayBinaryReader(buffer);
            var success = reader.ReadBoolean();
            var term = reader.ReadInt32();

            return new RequestVoteResponsePacket(new RequestVoteResponse(new Term(term), success));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<byte[]> ReadRequiredLengthAsync(Stream stream, int length, CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var left = length;
            var index = 0;
            while (0 < left)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(index, left), token);
                if (read == 0)
                {
                    throw new EndOfStreamException("Не удалось прочитать указанное количество байт");
                }

                left -= read;
                index += read;
            }

            return buffer;
        }
        catch (Exception)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    private static async ValueTask<RequestVoteRequestPacket> DeserializeRequestVoteRequestPacketAsync(
        Stream stream,
        CancellationToken token = default)
    {
        const int packetSize = sizeof(int)  // Id
                             + sizeof(int)  // Term
                             + sizeof(int)  // LogEntry Term
                             + sizeof(int); // LogEntry Index
        var buffer = await ReadRequiredLengthAsync(stream,
                         packetSize, token);
        try
        {
            var reader = new ArrayBinaryReader(buffer);
            var id = reader.ReadInt32();
            var term = reader.ReadInt32();
            var entryTerm = reader.ReadInt32();
            var entryIndex = reader.ReadInt32();
            return new RequestVoteRequestPacket(new RequestVoteRequest(new NodeId(id), new Term(term),
                new LogEntryInfo(new Term(entryTerm), entryIndex)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<ConnectResponsePacket> DeserializeConnectResponsePacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            var read = await stream.ReadAsync(buffer.AsMemory(0, 1), token);
            if (read == 0)
            {
                throw new SocketException(( int ) SocketError.Shutdown);
            }

            var reader = new ArrayBinaryReader(buffer);
            var success = reader.ReadBoolean();
            return new ConnectResponsePacket(success);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<ConnectRequestPacket> DeserializeConnectRequestPacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(sizeof(int));
        try
        {
            var left = sizeof(int);
            var index = 0;
            while (0 < left)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(index, left), token);
                left -= read;
                index += read;
            }

            var reader = new ArrayBinaryReader(buffer);
            var id = reader.ReadInt32();
            return new ConnectRequestPacket(new NodeId(id));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static ConnectRequestPacket DeserializeConnectRequestPacket(Stream stream, CancellationToken token)
    {
        return DeserializeConnectRequestPacketAsync(stream, token).GetAwaiter().GetResult();
    }
}