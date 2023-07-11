using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands;

public class DefaultResponseDeserializer
{
    public static readonly DefaultResponseDeserializer Instance = new();
    
    public IJobQueueResponse Deserialize(byte[] payload)
    {
        using var stream = new MemoryStream(payload);
        using var reader = new BinaryReader(stream);
        var marker = (ResponseType) reader.ReadInt32();
        return marker switch
               {
                   ResponseType.Dequeue  => DeserializeDequeueResponse(reader),
                   ResponseType.Enqueue  => DeserializeEnqueueResponse(reader),
                   ResponseType.GetCount => DeserializeGetCountResponse(reader),
                   ResponseType.Error    => DeserializeErrorResponse(reader),
                   _ => throw new ArgumentOutOfRangeException(nameof(marker), payload[0],
                            "Неизвестный тип ResponseType")
               };
    }

    private ErrorResponse DeserializeErrorResponse(BinaryReader reader)
    {
        var message = reader.ReadString();
        return new ErrorResponse(message);
    }

    private GetCountResponse DeserializeGetCountResponse(BinaryReader reader)
    {
        return new GetCountResponse(reader.ReadInt32());
    }

    private EnqueueResponse DeserializeEnqueueResponse(BinaryReader reader)
    {
        return new EnqueueResponse(reader.ReadBoolean());
    }

    private DequeueResponse DeserializeDequeueResponse(BinaryReader reader)
    {
        var success = reader.ReadBoolean();
        if (success)
        {
            var key = reader.ReadInt32();
            var bufferLength = reader.ReadInt32();
            var payload = new byte[bufferLength];
            var read = reader.Read(payload);
            if (read != bufferLength)
            {
                throw new InvalidDataException(
                    $"Прочитанный размер буфера меньше прочитанного количества байт. Прочитано: {read}. Указано: {bufferLength}");
            }
            
            return DequeueResponse.Ok(key, payload);
        }

        return DequeueResponse.Empty;
    }
}