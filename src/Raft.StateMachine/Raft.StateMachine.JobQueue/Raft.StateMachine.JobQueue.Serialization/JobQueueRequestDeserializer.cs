using Raft.StateMachine.JobQueue.Commands;
using Raft.StateMachine.JobQueue.Commands.Batch;
using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Serialization;

public class JobQueueRequestDeserializer : IJobQueueRequestDeserializer
{
    public static readonly JobQueueRequestDeserializer Instance = new();
    
    public IJobQueueRequest Deserialize(byte[] payload)
    {
        using var stream = new MemoryStream(payload);
        using var reader = new BinaryReader(stream);

        return Parse(reader);
    }

    private static IJobQueueRequest Parse(BinaryReader reader)
    {
        var marker = (RequestType) reader.ReadInt32();
        switch (marker)
        {
            case RequestType.EnqueueRequest:
                return ParseEnqueueRequest(reader);
            case RequestType.DequeueRequest:
                return ParseDequeueRequest(reader);
            case RequestType.GetCountRequest:
                return ParseGetCountRequest(reader);
            case RequestType.BatchRequest:
                return ParseBatchRequest(reader);
            default:
                throw new InvalidDataException($"Неизвестная команда: {marker}");
        }
    }

    private static BatchRequest ParseBatchRequest(BinaryReader reader)
    {
        var requestsCount = reader.ReadInt32();
        var requests = new List<IJobQueueRequest>();

        for (var i = 0; i < requestsCount; i++)
        {
            requests.Add(Parse(reader));
        }
        
        return new BatchRequest(requests);
    }

    private static GetCountRequest ParseGetCountRequest(BinaryReader _)
    {
        return GetCountRequest.Instance;
    }
    
    private static DequeueRequest ParseDequeueRequest(BinaryReader _)
    {
        return DequeueRequest.Instance;
    }
    
    private static EnqueueRequest ParseEnqueueRequest(BinaryReader reader)
    {
        var key = reader.ReadInt32();
        var bufferLength = reader.ReadInt32();
        var buffer = new byte[bufferLength];
        var read = reader.Read(buffer);
        if (read != bufferLength)
        {
            throw new InvalidDataException(
                $"Прочитанное количество байт из тела не равно указанному размеру тела. Указано: {bufferLength}. Прочитано: {read}");
        }

        return new EnqueueRequest(key, buffer);
    }
}