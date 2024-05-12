using System.Buffers;
using System.Text;
using TaskFlux.Utils.Serialization;

namespace TaskFlux.Network.Packets;

public class ErrorResponsePacket : Packet
{
    public int ErrorType { get; }
    public string Message { get; }
    public override PacketType Type => PacketType.ErrorResponse;

    public ErrorResponsePacket(int errorType, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        ErrorType = errorType;
        Message = message;
    }

    public override async ValueTask SerializeAsync(Stream stream, CancellationToken token)
    {
        var estimatedSize = sizeof(PacketType) // Маркер
                            + sizeof(int) // Код ошибки
                            + sizeof(int) // Длина строки
                            + Encoding.UTF8.GetByteCount(Message); // Сама строка
        var array = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            var buffer = array.AsMemory(0, estimatedSize);
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)PacketType.ErrorResponse);
            writer.Write(ErrorType);
            writer.Write(Message);
            await stream.WriteAsync(buffer, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    public new static async Task<ErrorResponsePacket> DeserializeAsync(Stream stream, CancellationToken token)
    {
        var reader = new StreamBinaryReader(stream);
        var type = await reader.ReadInt32Async(token);
        var message = await reader.ReadStringAsync(token);
        return new ErrorResponsePacket(type, message);
    }
}