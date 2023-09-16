using System.Buffers;

namespace Consensus.Network;

public abstract class RaftPacket
{
    internal RaftPacket()
    {
    }

    public abstract RaftPacketType PacketType { get; }
    protected abstract int EstimatePacketSize();
    protected abstract void SerializeBuffer(Span<byte> buffer);

    public async ValueTask SerializeAsync(Stream stream, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        var estimatedSize = EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            SerializeBuffer(buffer.AsSpan(0, estimatedSize));
            await stream.WriteAsync(buffer.AsMemory(0, estimatedSize), token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Serialize(Stream stream)
    {
        var estimatedSize = EstimatePacketSize();
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var span = buffer.AsSpan(0, estimatedSize);
            SerializeBuffer(span);
            stream.Write(span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}