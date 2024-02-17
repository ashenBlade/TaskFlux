namespace TaskFlux.Application.Cluster.Network;

internal static class SizeOf
{
    public const int Lsn = sizeof(Int64);
    public const int Term = sizeof(Int64);
    public const int NodeId = sizeof(Int32);
    public const int PacketType = sizeof(NodePacketType);
    public const int CheckSum = sizeof(UInt32);
    public const int ArrayLength = sizeof(Int32);
    public const int Int32 = sizeof(Int32);
    public const int Bool = sizeof(Byte);

    public static int Buffer(byte[] buffer) => sizeof(int) + buffer.Length;

    public static int BufferAligned(ReadOnlySpan<byte> buffer, int alignment) =>
        sizeof(int) + buffer.Length + GetAlignment(buffer.Length, alignment);

    private static int GetAlignment(int length, int alignment) => alignment == 0
                                                                      ? 0
                                                                      : length % alignment;

    public static int Buffer(ReadOnlySpan<byte> buffer) => sizeof(int) + buffer.Length;
    public static int Buffer(ReadOnlyMemory<byte> buffer) => sizeof(int) + buffer.Length;
}