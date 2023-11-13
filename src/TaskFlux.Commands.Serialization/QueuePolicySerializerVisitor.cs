using TaskFlux.Core.Policies;
using Utils.Serialization;

namespace TaskFlux.Commands.Serialization;

public class QueuePolicySerializerVisitor : IQueuePolicyVisitor<byte[]>
{
    public static readonly QueuePolicySerializerVisitor Instance = new();

    public byte[] Visit(PriorityRangeQueuePolicy policy)
    {
        const int size = sizeof(ResponseType) // Маркер
                       + sizeof(int)          // Код политики
                       + sizeof(long)         // Минимальный ключ
                       + sizeof(long);        // Максимальный ключ

        var buffer = new byte[size];

        var writer = new MemoryBinaryWriter(buffer);

        writer.Write(ResponseType.PolicyViolation);
        writer.Write(PolicyCode.PriorityRange);
        writer.Write(policy.Min);
        writer.Write(policy.Max);

        return buffer;
    }

    public byte[] Visit(MaxQueueSizeQueuePolicy policy)
    {
        const int size = sizeof(ResponseType) // Маркер
                       + sizeof(int)          // Код политики
                       + sizeof(int);         // Максимальный размер

        var buffer = new byte[size];

        var writer = new MemoryBinaryWriter(buffer);
        writer.Write(ResponseType.PolicyViolation);
        writer.Write(PolicyCode.MaxQueueSize);
        writer.Write(policy.MaxQueueSize);

        return buffer;
    }

    public byte[] Visit(MaxPayloadSizeQueuePolicy policy)
    {
        const int size = sizeof(ResponseType) // Маркер
                       + sizeof(int)          // Код политики
                       + sizeof(int);         // Максимальный размер

        var buffer = new byte[size];

        var writer = new MemoryBinaryWriter(buffer);
        writer.Write(ResponseType.PolicyViolation);
        writer.Write(PolicyCode.MaxPayloadSize);
        writer.Write(policy.MaxPayloadSize);

        return buffer;
    }
}