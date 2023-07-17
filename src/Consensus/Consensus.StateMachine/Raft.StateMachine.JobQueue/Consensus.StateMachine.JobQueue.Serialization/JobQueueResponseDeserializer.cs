using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.Error;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Serialization;

public class JobQueueResponseDeserializer
{
    public static readonly JobQueueResponseDeserializer Instance = new();
    
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
                   ResponseType.Batch => DeserializeBatchResponse(reader),
                   _ => throw new ArgumentOutOfRangeException(nameof(marker), payload[0],
                            "Неизвестный тип ResponseType")
               };
    }

    private IJobQueueResponse DeserializeResponse(BinaryReader reader)
    {
        var marker = (ResponseType) reader.ReadInt32();
        return marker switch
               {
                   ResponseType.Dequeue  => DeserializeDequeueResponse(reader),
                   ResponseType.Enqueue  => DeserializeEnqueueResponse(reader),
                   ResponseType.GetCount => DeserializeGetCountResponse(reader),
                   ResponseType.Error    => DeserializeErrorResponse(reader),
                   ResponseType.Batch    => DeserializeBatchResponse(reader),
                   _ => throw new ArgumentOutOfRangeException(nameof(marker), marker,
                            "Неизвестный тип ResponseType")
               };
    }

    private BatchResponse DeserializeBatchResponse(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        var responses = new List<IJobQueueResponse>(count);
        for (var i = 0; i < count; i++)
        {
            responses.Add(DeserializeResponse(reader));
        }

        return new BatchResponse(responses);
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