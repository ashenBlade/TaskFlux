using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands.Serializers;

public class DefaultRequestDeserializer
{
    public static readonly DefaultRequestDeserializer Instance = new();
    
    public IJobQueueRequest Deserialize(byte[] payload)
    {
        using var stream = new MemoryStream(payload);
        using var reader = new BinaryReader(stream);
        
        var marker = (RequestType) reader.ReadInt32();
        switch (marker)
        {
            case RequestType.EnqueueRequest:
                return ParseEnqueueRequest(reader);
            case RequestType.DequeueRequest:
                return ParseDequeueRequest(reader);
            case RequestType.GetCountRequest:
                return ParseGetCountRequest(reader);
            default:
                throw new InvalidDataException($"Неизвестная команда: {marker}");
        }
    }
    
    private GetCountRequest ParseGetCountRequest(BinaryReader _)
    {
        return GetCountRequest.Instance;
    }
    
    private DequeueRequest ParseDequeueRequest(BinaryReader _)
    {
        return DequeueRequest.Instance;
    }
    
    private EnqueueRequest ParseEnqueueRequest(BinaryReader reader)
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