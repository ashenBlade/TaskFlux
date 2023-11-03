using Consensus.Raft.Commands.AppendEntries;
using TaskFlux.Serialization.Helpers;
using Utils.CheckSum;

namespace Consensus.Network.Packets;

public class AppendEntriesRequestPacket : RaftPacket
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

    public override RaftPacketType PacketType => RaftPacketType.AppendEntriesRequest;

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
        writer.Write(( byte ) RaftPacketType.AppendEntriesRequest);
        writer.Write(buffer.Length
                   - (
                         sizeof(RaftPacketType) // Packet Type 
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
}