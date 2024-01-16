using TaskFlux.Consensus.Cluster.Network;
using TaskFlux.Consensus.Cluster.Network.Packets;

namespace TaskFlux.Consensus.Cluster;

public class NetworkSnapshot : ISnapshot
{
    private readonly PacketClient _client;

    public NetworkSnapshot(PacketClient client)
    {
        _client = client;
    }

    public IEnumerable<ReadOnlyMemory<byte>> GetAllChunks(CancellationToken token = default)
    {
        var requestPacket = new InstallSnapshotChunkResponsePacket();
        while (!token.IsCancellationRequested)
        {
            _client.Send(requestPacket);

            var packet = _client.Receive();
            switch (packet)
            {
                case {PacketType: NodePacketType.InstallSnapshotChunkRequest}:
                    var installChunkPacket = ( InstallSnapshotChunkRequestPacket ) packet;
                    if (installChunkPacket.Chunk.IsEmpty)
                    {
                        yield break;
                    }

                    yield return installChunkPacket.Chunk;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Получен неожиданный пакет данных. Ожидался {NodePacketType.InstallSnapshotChunkRequest}. Получен: {packet}");
            }
        }

        token.ThrowIfCancellationRequested();
    }
}