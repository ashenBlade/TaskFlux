using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Consensus.Network;
using Consensus.Network.Packets;
using Consensus.Peer.Exceptions;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Commands.RequestVote;
using Consensus.Raft.Persistence;
using TaskFlux.Models;
using Utils.CheckSum;
using Utils.Serialization;

namespace Consensus.Peer;

public class BinaryPacketDeserializer
{
    public static readonly BinaryPacketDeserializer Instance = new();

    /// <summary>
    /// Десериализовать пакет из переданного потока
    /// </summary>
    /// <param name="stream">Поток, из которого нужно десериализовать пакет</param>
    /// <returns>Десериализованный пакет</returns>
    /// <exception cref="EndOfStreamException">Соединение было закрыто во время чтения пакета</exception>
    /// <exception cref="IntegrityException">При чтении пакета с пользовательскими данными обнаружена ошибка</exception>
    /// <exception cref="UnknownPacketException">Получен неизвестный маркер пакета</exception>
    public RaftPacket Deserialize(Stream stream)
    {
        Span<byte> array = stackalloc byte[1];
        var read = stream.Read(array);
        if (read == 0)
        {
            throw new EndOfStreamException("Соединение было закрыто во время чтения маркера пакета");
        }

        var packetType = ( RaftPacketType ) array[0];
        try
        {
            return packetType switch
                   {
                       RaftPacketType.AppendEntriesRequest    => DeserializeAppendEntriesRequestPacket(stream),
                       RaftPacketType.AppendEntriesResponse   => DeserializeAppendEntriesResponsePacket(stream),
                       RaftPacketType.RequestVoteResponse     => DeserializeRequestVoteResponsePacket(stream),
                       RaftPacketType.RequestVoteRequest      => DeserializeRequestVoteRequestPacket(stream),
                       RaftPacketType.ConnectRequest          => DeserializeConnectRequestPacket(stream),
                       RaftPacketType.ConnectResponse         => DeserializeConnectResponsePacket(stream),
                       RaftPacketType.InstallSnapshotChunk    => DeserializeInstallSnapshotChunkPacket(stream),
                       RaftPacketType.InstallSnapshotResponse => DeserializeInstallSnapshotResponsePacket(stream),
                       RaftPacketType.InstallSnapshotRequest  => DeserializeInstallSnapshotRequestPacket(stream),
                       RaftPacketType.RetransmitRequest       => DeserializeRetransmitRequestPacket(stream),
                   };
        }
        catch (SwitchExpressionException)
        {
            throw new UnknownPacketException(array[0]);
        }
    }


    /// <summary>
    /// Десериализовать пакет из переданного потока
    /// </summary>
    /// <param name="stream">Поток, из которого нужно десериализовать пакет</param>
    /// <param name="token">Токен отмены</param>
    /// <returns>Десериализованный пакет</returns>
    /// <exception cref="EndOfStreamException">Соединение было закрыто во время чтения пакета</exception>
    /// <exception cref="IntegrityException">При чтении пакета с пользовательскими данными обнаружена ошибка</exception>
    /// <exception cref="UnknownPacketException">Получен неизвестный маркер пакета</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token"/> был отменен</exception>
    public async ValueTask<RaftPacket> DeserializeAsync(Stream stream, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();

        var (packetType, marker) = await ReadMarkerAsync(stream, token);

        try
        {
            return packetType switch
                   {
                       RaftPacketType.AppendEntriesRequest =>
                           await DeserializeAppendEntriesRequestPacketAsync(stream, token),
                       RaftPacketType.AppendEntriesResponse =>
                           await DeserializeAppendEntriesResponsePacketAsync(stream, token),
                       RaftPacketType.RequestVoteRequest =>
                           await DeserializeRequestVoteRequestPacketAsync(stream, token),
                       RaftPacketType.RequestVoteResponse =>
                           await DeserializeRequestVoteResponsePacketAsync(stream, token),
                       RaftPacketType.ConnectRequest =>
                           await DeserializeConnectRequestPacketAsync(stream, token),
                       RaftPacketType.ConnectResponse =>
                           await DeserializeConnectResponsePacketAsync(stream, token),
                       RaftPacketType.InstallSnapshotChunk =>
                           await DeserializeInstallSnapshotChunkPacketAsync(stream, token),
                       RaftPacketType.InstallSnapshotRequest =>
                           await DeserializeInstallSnapshotRequestPacketAsync(stream, token),
                       RaftPacketType.InstallSnapshotResponse =>
                           await DeserializeInstallSnapshotResponsePacketAsync(stream, token),
                       RaftPacketType.RetransmitRequest =>
                           await DeserializeRetransmitRequestPacketAsync(stream, token),
                   };
        }
        catch (SwitchExpressionException)
        {
            throw new UnknownPacketException(marker);
        }

        static async ValueTask<(RaftPacketType PacketType, byte Marker)> ReadMarkerAsync(
            Stream stream,
            CancellationToken token)
        {
            var array = ArrayPool<byte>.Shared.Rent(1);
            try
            {
                var read = await stream.ReadAsync(array.AsMemory(0, 1), token);
                if (read == 0)
                {
                    throw new EndOfStreamException("Соединение было закрыто во время чтения маркера пакета");
                }

                var marker = array[0];
                return ( ( RaftPacketType ) marker, marker );
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }
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
        // Читаем размер данных
        Span<byte> headerBuffer = stackalloc byte[4];
        FillBuffer(stream, headerBuffer);
        var payloadLength = new SpanBinaryReader(headerBuffer)
           .ReadInt32();

        var leftPacketSize = payloadLength + sizeof(uint);
        var buffer = ArrayPool<byte>.Shared.Rent(leftPacketSize);
        try
        {
            // Читаем оставшийся пакет
            var dataSpan = buffer.AsSpan(0, leftPacketSize);
            FillBuffer(stream, dataSpan);
            // Десериализуем
            return DeserializeInstallSnapshotChunkPacket(dataSpan);
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
        // Читаем, сколько в буфере занимают данные
        int length;
        var (buffer, memory) = await ReadRequiredLengthAsync(stream, sizeof(int), token);
        try
        {
            length = new SpanBinaryReader(memory.Span).ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var totalPacketSize = length        // Данные
                            + sizeof(uint); // Чек-сумма

        // Читаем оставшийся пакет
        buffer = ArrayPool<byte>.Shared.Rent(totalPacketSize);
        try
        {
            await FillBufferAsync(stream, buffer.AsMemory(0, totalPacketSize), token);
            // Десериализуем пакет
            return DeserializeInstallSnapshotChunkPacket(buffer.AsSpan(0, totalPacketSize));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static InstallSnapshotChunkPacket DeserializeInstallSnapshotChunkPacket(Span<byte> payload)
    {
        Debug.Assert(payload.Length >= 4, "payload.Length >= 4",
            "Размер данных не может быть меньше 4 - чек-сумма занимает минимум 4 байта");
        ValidateChecksum(payload);

        if (payload.Length == 4)
        {
            return new InstallSnapshotChunkPacket(Array.Empty<byte>());
        }

        return new InstallSnapshotChunkPacket(payload[..^4].ToArray());

        static void ValidateChecksum(Span<byte> buffer)
        {
            var crcReader = new SpanBinaryReader(buffer[^4..]);
            var storedCrc = crcReader.ReadUInt32();
            var calculatedCrc = Crc32CheckSum.Compute(buffer[..^4]);
            if (storedCrc != calculatedCrc)
            {
                throw new IntegrityException();
            }
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

    private static AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(Stream stream)
    {
        // Сначала определим полный размер пакета
        // ReSharper disable once RedundantAssignment
        var (buffer, memory) = ReadRequiredLength(stream, sizeof(int));
        int payloadSize;
        try
        {
            var reader = new SpanBinaryReader(memory.Span);
            payloadSize = reader.ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        // Запишем все байты и десериализуем пакет
        buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            var dataSpan = buffer.AsSpan(0, payloadSize);
            FillBuffer(stream, dataSpan);
            return DeserializeAppendEntriesRequestPacket(dataSpan);
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
        // Временный массив для 
        int payloadSize;
        var (buffer, _) = await ReadRequiredLengthAsync(stream, sizeof(int), token);
        try
        {
            var reader = new ArrayBinaryReader(buffer);
            payloadSize = reader.ReadInt32();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            await FillBufferAsync(stream, buffer.AsMemory(0, payloadSize), token);
            return DeserializeAppendEntriesRequestPacket(buffer.AsSpan(0, payloadSize));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static AppendEntriesRequestPacket DeserializeAppendEntriesRequestPacket(Span<byte> buffer)
    {
        // Сперва валидируем чек-сумму
        ValidateCheckSum(buffer);

        var reader = new SpanBinaryReader(buffer);

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

        static void ValidateCheckSum(Span<byte> buffer)
        {
            var crcReader = new SpanBinaryReader(buffer[^4..]);
            var storedCrc = crcReader.ReadUInt32();
            const int dataStartPosition = 4  // Терм  
                                        + 4  // Id лидера  
                                        + 4  // Коммит лидера
                                        + 4  // Терм последней записи
                                        + 4  // Индекс последней записи
                                        + 4; // Количество записей
            var calculatedCrc = Crc32CheckSum.Compute(buffer[dataStartPosition..^4]);
            if (storedCrc != calculatedCrc)
            {
                throw new IntegrityException();
            }
        }
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

    #region RetransmitRequest

    // ReSharper disable once UnusedParameter.Local
    private RaftPacket DeserializeRetransmitRequestPacket(Stream stream)
    {
        return new RetransmitRequestPacket();
    }

    // ReSharper disable once UnusedParameter.Local
    private ValueTask<RaftPacket> DeserializeRetransmitRequestPacketAsync(Stream stream, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return new ValueTask<RaftPacket>(new RetransmitRequestPacket());
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
            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), token);
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
            stream.ReadExactly(buffer.AsSpan(0, length));
            return ( buffer, buffer.AsMemory(0, length) );
        }
        catch (Exception)
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    private static void FillBuffer(Stream stream, Span<byte> buffer)
    {
        stream.ReadExactly(buffer);
    }

    private static async ValueTask FillBufferAsync(Stream stream, Memory<byte> buffer, CancellationToken token)
    {
        await stream.ReadExactlyAsync(buffer, token);
    }
}