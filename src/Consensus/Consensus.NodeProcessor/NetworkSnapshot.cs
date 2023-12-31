using Consensus.Core;
using Consensus.Network;
using Consensus.Network.Packets;

namespace Consensus.NodeProcessor;

public class NetworkSnapshot : ISnapshot
{
    private readonly PacketClient _client;

    public NetworkSnapshot(PacketClient client)
    {
        _client = client;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        while (!token.IsCancellationRequested)
        {
            var packet = _client.Receive();
            switch (packet)
            {
                case {PacketType: RaftPacketType.InstallSnapshotChunk}:
                    var installChunkPacket = ( InstallSnapshotChunkPacket ) packet;
                    if (installChunkPacket.Chunk.IsEmpty)
                    {
                        yield break;
                    }

                    yield return installChunkPacket.Chunk;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Получен неожиданный пакет данных. Ожидался {RaftPacketType.InstallSnapshotChunk}. Получен: {packet}");
            }
        }
    }
}