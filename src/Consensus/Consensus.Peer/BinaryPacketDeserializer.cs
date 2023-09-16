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

    public RaftPacket Deserialize(Stream stream)
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
                   RaftPacketType.ConnectRequest          => DeserializeConnectRequestPacket(stream),
                   RaftPacketType.ConnectResponse         => DeserializeConnectResponsePacket(stream),
                   RaftPacketType.RequestVoteRequest      => DeserializeRequestVoteRequestPacket(stream),
                   RaftPacketType.RequestVoteResponse     => DeserializeRequestVoteResponsePacket(stream),
                   RaftPacketType.AppendEntriesRequest    => DeserializeAppendEntriesRequestPacket(stream),
                   RaftPacketType.AppendEntriesResponse   => DeserializeAppendEntriesResponsePacket(stream),
                   RaftPacketType.InstallSnapshotRequest  => DeserializeInstallSnapshotRequestPacket(stream),
                   RaftPacketType.InstallSnapshotChunk    => DeserializeInstallSnapshotChunkPacket(stream),
                   RaftPacketType.InstallSnapshotResponse => DeserializeInstallSnapshotResponsePacket(stream),
               };
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
                   RaftPacketType.InstallSnapshotChunk => await DeserializeInstallSnapshotChunkPacketAsync(stream,
                                                              token),
                   RaftPacketType.InstallSnapshotRequest => await DeserializeInstallSnapshotRequestPacketAsync(stream,
                                                                token),
                   RaftPacketType.InstallSnapshotResponse => await DeserializeInstallSnapshotResponsePacketAsync(stream,
                                                                 token),
               };
    }

    #region InstallSnapshotResponse

    private static InstallSnapshotResponsePacket DeserializeInstallSnapshotResponsePacket(
        Stream stream)
    {
        var (buffer, memory) = ReadRequiredLength(stream, sizeof(int));
        try
        {
            return DeserializeInstallSnapshotResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<InstallSnapshotResponsePacket> DeserializeInstallSnapshotResponsePacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, sizeof(int), token);
        try
        {
            return DeserializeInstallSnapshotResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static InstallSnapshotResponsePacket DeserializeInstallSnapshotResponsePacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);

        var term = reader.ReadInt32();

        return new InstallSnapshotResponsePacket(new Term(term));
    }

    #endregion


    #region InstallSnapshotChunk

    private static InstallSnapshotChunkPacket DeserializeInstallSnapshotChunkPacket(Stream stream)
    {
        var (buffer, memory) = ReadRequiredLength(stream, sizeof(int));
        int length;
        try
        {
            length = new SpanBinaryReader(memory.Span).ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (length == 0)
        {
            return new InstallSnapshotChunkPacket(Array.Empty<byte>());
        }

        ( buffer, memory ) = ReadRequiredLength(stream, length);
        try
        {
            return new InstallSnapshotChunkPacket(memory.ToArray());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<InstallSnapshotChunkPacket> DeserializeInstallSnapshotChunkPacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, sizeof(int), token);
        int length;
        try
        {
            length = new SpanBinaryReader(memory.Span).ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (length == 0)
        {
            return new InstallSnapshotChunkPacket(Array.Empty<byte>());
        }

        ( buffer, memory ) = await ReadRequiredLengthAsync(stream, length, token);
        try
        {
            return new InstallSnapshotChunkPacket(memory.ToArray());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    #endregion


    #region InstallSnapshotRequest

    private static InstallSnapshotRequestPacket DeserializeInstallSnapshotRequestPacket(Stream stream)
    {
        const int size = sizeof(int)  // Терм
                       + sizeof(int)  // Id лидера
                       + sizeof(int)  // Последний индекс
                       + sizeof(int); // Последний терм
        var (buffer, memory) = ReadRequiredLength(stream, size);
        try
        {
            return DeserializeInstallSnapshotRequestPacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<InstallSnapshotRequestPacket> DeserializeInstallSnapshotRequestPacketAsync(
        Stream stream,
        CancellationToken token)
    {
        const int size = sizeof(int)  // Терм
                       + sizeof(int)  // Id лидера
                       + sizeof(int)  // Последний индекс
                       + sizeof(int); // Последний терм
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, size, token);
        try
        {
            return DeserializeInstallSnapshotRequestPacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static InstallSnapshotRequestPacket DeserializeInstallSnapshotRequestPacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);

        var term = reader.ReadInt32();
        var leaderId = reader.ReadInt32();
        var lastIndex = reader.ReadInt32();
        var lastTerm = reader.ReadInt32();

        return new InstallSnapshotRequestPacket(new Term(term), new NodeId(leaderId),
            new LogEntryInfo(new Term(lastTerm), lastIndex));
    }

    #endregion


    #region AppendEntriesResponse

    private AppendEntriesResponsePacket DeserializeAppendEntriesResponsePacket(Stream stream)
    {
        var (buffer, memory) = ReadRequiredLength(stream, sizeof(bool) + sizeof(int));
        try
        {
            return DeserializeAppendEntriesResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<AppendEntriesResponsePacket> DeserializeAppendEntriesResponsePacketAsync(
        Stream stream,
        CancellationToken token)
    {
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, sizeof(bool) + sizeof(int), token);
        try
        {
            return DeserializeAppendEntriesResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static AppendEntriesResponsePacket DeserializeAppendEntriesResponsePacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);

        var success = reader.ReadBoolean();
        var term = reader.ReadInt32();

        return new AppendEntriesResponsePacket(new AppendEntriesResponse(new Term(term), success));
    }

    #endregion


    #region AppendEntriesRequest

    private AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(Stream stream)
    {
        // ReSharper disable once RedundantAssignment
        var (buffer, memory) = ReadRequiredLength(stream, sizeof(int));
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

        ( buffer, memory ) = ReadRequiredLength(stream, totalPacketLength);
        try
        {
            return DeserializeAppendEntriesRequestPacket(memory);
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
        // ReSharper disable once RedundantAssignment
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, sizeof(int), token);
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

        ( buffer, memory ) = await ReadRequiredLengthAsync(stream, totalPacketLength, token);
        try
        {
            return DeserializeAppendEntriesRequestPacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);

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

    #endregion


    #region RequestVoteResponse

    private RequestVoteResponsePacket DeserializeRequestVoteResponsePacket(Stream stream)
    {
        const int packetSize = sizeof(bool) // Success 
                             + sizeof(int); // Term
        var (buffer, memory) = ReadRequiredLength(stream, packetSize);
        try
        {
            return DeserializeRequestVoteResponsePacket(memory);
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
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, packetSize, token);
        try
        {
            return DeserializeRequestVoteResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static RequestVoteResponsePacket DeserializeRequestVoteResponsePacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);
        var success = reader.ReadBoolean();
        var term = reader.ReadInt32();

        return new RequestVoteResponsePacket(new RequestVoteResponse(new Term(term), success));
    }

    #endregion


    #region RequestVoteRequest

    private RequestVoteRequestPacket DeserializeRequestVoteRequestPacket(Stream stream)
    {
        const int packetSize = sizeof(int)  // Id
                             + sizeof(int)  // Term
                             + sizeof(int)  // LogEntry Term
                             + sizeof(int); // LogEntry Index
        var (buffer, memory) = ReadRequiredLength(stream, packetSize);
        try
        {
            return DeserializeRequestVoteRequest(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
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
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, packetSize, token);
        try
        {
            return DeserializeRequestVoteRequest(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static RequestVoteRequestPacket DeserializeRequestVoteRequest(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);
        var id = reader.ReadInt32();
        var term = reader.ReadInt32();
        var entryTerm = reader.ReadInt32();
        var entryIndex = reader.ReadInt32();
        return new RequestVoteRequestPacket(new RequestVoteRequest(new NodeId(id), new Term(term),
            new LogEntryInfo(new Term(entryTerm), entryIndex)));
    }

    #endregion


    #region ConnectRequest

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

    private static ConnectRequestPacket DeserializeConnectRequestPacket(Stream stream)
    {
        return DeserializeConnectRequestPacketAsync(stream, CancellationToken.None).GetAwaiter().GetResult();
    }

    #endregion


    #region ConnectResponse

    private ConnectResponsePacket DeserializeConnectResponsePacket(Stream stream)
    {
        var (buffer, memory) = ReadRequiredLength(stream, 1);
        try
        {
            return DeserializeConnectResponsePacket(memory);
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
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, 1, token);
        try
        {
            return DeserializeConnectResponsePacket(memory);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static ConnectResponsePacket DeserializeConnectResponsePacket(Memory<byte> buffer)
    {
        var reader = new SpanBinaryReader(buffer.Span);
        var success = reader.ReadBoolean();
        return new ConnectResponsePacket(success);
    }

    #endregion


    private static async ValueTask<(byte[], Memory<byte>)> ReadRequiredLengthAsync(
        Stream stream,
        int length,
        CancellationToken token)
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

            return ( buffer, buffer.AsMemory(0, length) );
        }
        catch (Exception)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    private static (byte[], Memory<byte>) ReadRequiredLength(Stream stream, int length)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var left = length;
            var index = 0;
            while (0 < left)
            {
                var read = stream.Read(buffer.AsSpan(index, left));
                if (read == 0)
                {
                    throw new EndOfStreamException("Не удалось прочитать указанное количество байт");
                }

                left -= read;
                index += read;
            }

            return ( buffer, buffer.AsMemory(0, length) );
        }
        catch (Exception)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }
}