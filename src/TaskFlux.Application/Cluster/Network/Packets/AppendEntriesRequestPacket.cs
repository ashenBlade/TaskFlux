using System.Buffers;
using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Consensus.Persistence;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class AppendEntriesRequestPacket : NodePacket
{
    /// <summary>
    /// Позиция с которой начинаются сами данные.
    /// Нужно для тестов
    /// </summary>
    internal const int DataStartPosition = BasePacketSize - SizeOf.CheckSum;

    public override NodePacketType PacketType => NodePacketType.AppendEntriesRequest;

    private const int BasePacketSize = SizeOf.PacketType  // Маркер
                                     + SizeOf.Int32       // Размер
                                     + SizeOf.NodeId      // ID узла лидера
                                     + SizeOf.Lsn         // Коммит лидера
                                     + SizeOf.Term        // Терм лидера
                                     + SizeOf.Term        // Терм предыдущей записи
                                     + SizeOf.Lsn         // LSN предыдущей записи
                                     + SizeOf.ArrayLength // Количество записей в массиве
                                     + SizeOf.CheckSum;   // Чек-сумма 

    protected override int EstimatePacketSize()
    {
        var entries = Request.Entries;
        if (entries.Count == 0)
        {
            return BasePacketSize;
        }

        return BasePacketSize
             + entries.Sum(entry => SizeOf.Term                 // Терм записи
                                  + SizeOf.Buffer(entry.Data)); // Размер буфера с данными включая длину
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(NodePacketType.AppendEntriesRequest);

        // Буфер, который нам передали должен занимать ровно столько сколько сказали,
        // поэтому размер нагрузки равен размеру буфера за исключением размера маркера пакета и самого поля длины
        var packetPayloadLength = buffer.Length - ( SizeOf.PacketType + SizeOf.ArrayLength );
        writer.Write(packetPayloadLength);

        writer.Write(Request.Term);
        writer.Write(Request.LeaderId);
        writer.Write(Request.LeaderCommit);
        writer.Write(Request.PrevLogEntryInfo.Term);
        writer.Write(Request.PrevLogEntryInfo.Index);
        writer.Write(Request.Entries.Count);

        var dataStartPosition = writer.Index;
        if (Request.Entries.Count > 0)
        {
            foreach (var entry in Request.Entries)
            {
                writer.Write(entry.Term);
                writer.WriteBuffer(entry.Data);
            }
        }

        var checkSum = Crc32CheckSum.Compute(buffer[dataStartPosition..writer.Index]);
        writer.Write(checkSum);
    }

    public AppendEntriesRequest Request { get; }

    public AppendEntriesRequestPacket(AppendEntriesRequest request)
    {
        Request = request;
    }

    /// <summary>
    /// Получить индекс на котором в сериализованном буфере заканчиваются данные.
    /// Нужен для тестов на проверку целостности
    /// </summary>
    internal int GetDataEndPosition()
    {
        return DataStartPosition
             + Request.Entries.Sum(entry => SizeOf.Term                 // Терм записи
                                          + SizeOf.Buffer(entry.Data)); // Данные записи
    }

    public new static AppendEntriesRequestPacket Deserialize(Stream stream)
    {
        // Сначала определим полный размер пакета
        // ReSharper disable once RedundantAssignment
        var streamReader = new StreamBinaryReader(stream);
        var payloadSize = streamReader.ReadInt32();

        if (payloadSize < 1024) // 1 Кб
        {
            Span<byte> span = stackalloc byte[payloadSize];
            stream.ReadExactly(span);
            return DeserializePacketVerifyCheckSum(span);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            var data = buffer.AsSpan(0, payloadSize);
            stream.ReadExactly(data);
            return DeserializePacketVerifyCheckSum(data);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public new static async Task<AppendEntriesRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        // Сначала определим полный размер пакета
        // ReSharper disable once RedundantAssignment
        var streamReader = new StreamBinaryReader(stream);
        var payloadSize = await streamReader.ReadInt32Async(token);
        var buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            var data = buffer.AsMemory(0, payloadSize);
            await stream.ReadExactlyAsync(data, token);
            return DeserializePacketVerifyCheckSum(data.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static AppendEntriesRequestPacket DeserializePacketVerifyCheckSum(Span<byte> buffer)
    {
        // Сперва валидируем чек-сумму
        ValidateCheckSum(buffer);

        var reader = new SpanBinaryReader(buffer);

        var term = reader.ReadTerm();
        var leaderId = reader.ReadNodeId();
        var leaderCommit = reader.ReadLsn();
        var lastEntryTerm = reader.ReadTerm();
        var lastEntryIndex = reader.ReadLsn();
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
                var logEntryTerm = reader.ReadTerm();
                var payload = reader.ReadBuffer();
                list.Add(new LogEntry(logEntryTerm, payload));
            }

            entries = list;
        }

        return new AppendEntriesRequestPacket(new AppendEntriesRequest(term, leaderCommit, leaderId,
            new LogEntryInfo(lastEntryTerm, lastEntryIndex), entries));

        static void ValidateCheckSum(Span<byte> buffer)
        {
            var crcReader = new SpanBinaryReader(buffer[^4..]);
            var storedCrc = crcReader.ReadUInt32();
            const int dataStartPosition = SizeOf.Term         // Терм  
                                        + SizeOf.NodeId       // Id лидера  
                                        + SizeOf.Lsn          // Коммит лидера
                                        + SizeOf.Term         // Терм последней записи
                                        + SizeOf.Lsn          // Индекс последней записи
                                        + SizeOf.ArrayLength; // Количество записей
            var calculatedCrc = Crc32CheckSum.Compute(buffer[dataStartPosition..^4]);
            if (storedCrc != calculatedCrc)
            {
                throw new IntegrityException();
            }
        }
    }
}