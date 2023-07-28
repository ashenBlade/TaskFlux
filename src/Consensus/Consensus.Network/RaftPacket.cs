using System.Buffers;

namespace Consensus.Network;

public abstract class RaftPacket
{
    internal RaftPacket() 
    { }
    
    public abstract RaftPacketType PacketType { get; }
    protected abstract int EstimatePacketSize();
    public async ValueTask Serialize(Stream stream, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var totalSize = EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
        try
        {
            SerializeBuffer(buffer.AsSpan(0, totalSize));
            await stream.WriteAsync(buffer.AsMemory(0, totalSize), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    protected abstract void SerializeBuffer(Span<byte> buffer);
}