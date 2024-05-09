using TaskFlux.Consensus;
using TaskFlux.Consensus.Cluster.Network.Exceptions;
using TaskFlux.Consensus.Commands.AppendEntries;
using TaskFlux.Utils.CheckSum;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Application.Cluster.Network.Packets;

public class AppendEntriesRequestPacket : NodePacket
{
    /// <summary>
    /// Позиция с которой начинаются сами данные.
    /// Нужно для тестов
    /// </summary>
    internal const int DataStartPosition = BasePayloadSize - SizeOf.CheckSum;

    public override NodePacketType PacketType => NodePacketType.AppendEntriesRequest;

    private const int BasePayloadSize = SizeOf.Int32 // Размер полезной нагрузки
                                        + SizeOf.Lsn // Коммит лидера
                                        + SizeOf.Term // Терм лидера
                                        + SizeOf.Term // Терм предыдущей записи
                                        + SizeOf.Lsn // LSN предыдущей записи
                                        + SizeOf.NodeId // ID узла лидера
                                        + SizeOf.ArrayLength; // Количество записей в массиве

    private const int DataAlignment = 8;


    protected override int EstimatePayloadSize()
    {
        var entries = Request.Entries;
        if (entries.Count == 0)
        {
            return BasePayloadSize;
        }

        return BasePayloadSize
               + entries.Sum(entry => SizeOf.Term // Терм записи
                                      + SizeOf.BufferAligned(entry.Data,
                                          DataAlignment)); // Размер буфера с данными включая длину
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);

        // Буфер, который нам передали должен занимать ровно столько сколько сказали,
        // поэтому размер нагрузки равен размеру буфера за исключением размера самой нагрузки
        var packetPayloadLength = buffer.Length - SizeOf.Int32;
        writer.Write(packetPayloadLength);

        writer.Write(Request.LeaderCommit);
        writer.Write(Request.Term);
        writer.Write(Request.PrevLogEntryInfo.Term);
        writer.Write(Request.PrevLogEntryInfo.Index);
        writer.Write(Request.LeaderId);
        writer.Write(Request.Entries.Count);

        if (Request.Entries.Count > 0)
        {
            foreach (var entry in Request.Entries)
            {
                writer.Write(entry.Term);
                writer.WriteBufferAligned(entry.Data, DataAlignment);
            }
        }
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
               + Request.Entries.Sum(entry => SizeOf.Term // Терм записи
                                              + SizeOf.BufferAligned(entry.Data, DataAlignment)) // Данные записи
               + sizeof(uint); // Чек-сумма
    }

    public new static AppendEntriesRequestPacket Deserialize(Stream stream)
    {
        // Сначала определим полный размер пакета
        // ReSharper disable once RedundantAssignment
        Span<byte> prefixSpan = stackalloc byte[sizeof(int)];
        stream.ReadExactly(prefixSpan);
        var payloadSize = new SpanBinaryReader(prefixSpan).ReadInt32() + sizeof(uint);

        if (payloadSize < 1024) // 1 Кб
        {
            Span<byte> span = stackalloc byte[payloadSize];
            stream.ReadExactly(span);
            return DeserializePayload(span, prefixSpan);
        }

        using var buffer = Rent(payloadSize);
        var data = buffer.GetSpan();
        stream.ReadExactly(data);
        return DeserializePayload(data, prefixSpan);
    }

    public new static async Task<AppendEntriesRequestPacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        using var prefixBuffer = Rent(sizeof(int));
        await stream.ReadExactlyAsync(prefixBuffer.GetMemory(), token);
        var totalPayloadSize = new SpanBinaryReader(prefixBuffer.GetSpan()).ReadInt32();

        using var payloadBuffer = Rent(totalPayloadSize + sizeof(uint));
        await stream.ReadExactlyAsync(payloadBuffer.GetMemory(), token);
        return DeserializePayload(payloadBuffer.GetSpan(), prefixBuffer.GetSpan());
    }

    private static AppendEntriesRequestPacket DeserializePayload(Span<byte> buffer, Span<byte> prefix)
    {
        VerifyCheckSumRequest(buffer, prefix);

        var reader = new SpanBinaryReader(buffer);

        var leaderCommit = reader.ReadLsn();
        var term = reader.ReadTerm();
        var lastEntryTerm = reader.ReadTerm();
        var lastEntryIndex = reader.ReadLsn();
        var leaderId = reader.ReadNodeId();
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
                var payload = reader.ReadBufferAligned(DataAlignment);
                list.Add(new LogEntry(logEntryTerm, payload));
            }

            entries = list;
        }

        return new AppendEntriesRequestPacket(new AppendEntriesRequest(term, leaderCommit, leaderId,
            new LogEntryInfo(lastEntryTerm, lastEntryIndex), entries));
    }

    private static void VerifyCheckSumRequest(Span<byte> payload, Span<byte> payloadSizePrefix)
    {
        var calculated = Crc32CheckSum.Compute(payloadSizePrefix);
        calculated = Crc32CheckSum.Compute(calculated, payload[..^4]);
        var stored = new SpanBinaryReader(payload[^4..]).ReadUInt32();
        if (calculated != stored)
        {
            throw new IntegrityException();
        }
    }
}