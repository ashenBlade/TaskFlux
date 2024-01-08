using System.Buffers;
using Consensus.Peer.Exceptions;
using Consensus.Raft;
using Consensus.Raft.Commands.AppendEntries;
using Consensus.Raft.Persistence;
using TaskFlux.Models;
using Utils.CheckSum;
using Utils.Serialization;

namespace Consensus.Network.Packets;

public class AppendEntriesRequestPacket : NodePacket
{
    /// <summary>
    /// Позиция с которой начинаются сами данные.
    /// Нужно для тестов
    /// </summary>
    internal const int DataStartPosition = 1  // Маркер
                                         + 4  // Размер
                                         + 4  // Leader Id
                                         + 4  // LeaderCommit 
                                         + 4  // Term
                                         + 4  // PrevLogEntry Term
                                         + 4  // PrevLogEntry Index
                                         + 4; // Entries Count

    public override NodePacketType PacketType => NodePacketType.AppendEntriesRequest;

    protected override int EstimatePacketSize()
    {
        const int baseSize = 1  // Маркер
                           + 4  // Размер
                           + 4  // Leader Id
                           + 4  // LeaderCommit 
                           + 4  // Term
                           + 4  // PrevLogEntry Term
                           + 4  // PrevLogEntry Index
                           + 4  // Entries Count
                           + 4; // Чек-сумма

        var entries = Request.Entries;
        if (entries.Count == 0)
        {
            return baseSize;
        }

        return baseSize
             + entries.Sum(entry => 4 // Term
                                  + 4 // Размер
                                  + entry.Data.Length);
    }

    protected override void SerializeBuffer(Span<byte> buffer)
    {
        var writer = new SpanBinaryWriter(buffer);
        writer.Write(( byte ) NodePacketType.AppendEntriesRequest);
        writer.Write(buffer.Length
                   - (
                         sizeof(NodePacketType) // Packet Type 
                       + sizeof(int)            // Length
                     ));

        writer.Write(Request.Term.Value);
        writer.Write(Request.LeaderId.Id);
        writer.Write(Request.LeaderCommit);
        writer.Write(Request.PrevLogEntryInfo.Term.Value);
        writer.Write(Request.PrevLogEntryInfo.Index);
        writer.Write(Request.Entries.Count);

        var dataStartPosition = writer.Index;
        if (Request.Entries.Count > 0)
        {
            foreach (var entry in Request.Entries)
            {
                writer.Write(entry.Term.Value);
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
             + Request.Entries.Sum(entry => 4 // Term
                                          + 4 // Размер
                                          + entry.Data.Length);
    }

    public new static AppendEntriesRequestPacket Deserialize(Stream stream)
    {
        // Сначала определим полный размер пакета
        // ReSharper disable once RedundantAssignment
        var streamReader = new StreamBinaryReader(stream);
        var payloadSize = streamReader.ReadInt32();

        // Запишем все байты и десериализуем пакет
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

        // Запишем все байты и десериализуем пакет
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
}