using System.ComponentModel;
using Consensus.Core;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace TaskFlux.Requests.Serialization;

public class RequestSerializer: ISerializer<IRequest>
{
    public static readonly RequestSerializer Instance = new();
    public byte[] Serialize(IRequest command)
    {
        var memory = new MemoryStream();
        var writer = new BinaryWriter(memory);
        var visitor = new RequestSerializerVisitor(writer);
        command.Accept(visitor);
        return memory.ToArray();
    }
    
    // Лучше будет добавить указание размера изначально
    private class RequestSerializerVisitor : IRequestVisitor
    {
        private readonly BinaryWriter _writer;

        public RequestSerializerVisitor(BinaryWriter writer)
        {
            _writer = writer;
        }
        
        public void Visit(GetCountRequest getCountRequest)
        {
            _writer.Write((int)RequestType.GetCountRequest);
        }

        public void Visit(EnqueueRequest enqueueRequest)
        {
            _writer.Write((int)RequestType.EnqueueRequest);
            _writer.Write(enqueueRequest.Key);
            _writer.Write(enqueueRequest.Payload.Length);
            _writer.Write(enqueueRequest.Payload);
        }

        public void Visit(DequeueRequest dequeueRequest)
        {
            _writer.Write((int)RequestType.DequeueRequest);
        }

        public void Visit(BatchRequest batchRequest)
        {
            _writer.Write((int)RequestType.BatchRequest);
            _writer.Write(batchRequest.Requests.Count);
            foreach (var request in batchRequest.Requests)
            {
                request.Accept(this);
            }
        }
    }

    public IRequest Deserialize(byte[] payload)
    {
        var memory = new MemoryStream(payload);
        var reader = new BinaryReader(memory);
        return Deserialize(reader);
    }

    private static IRequest Deserialize(BinaryReader reader)
    {
        var marker = ( RequestType ) reader.ReadInt32();
        return marker switch
               {
                   RequestType.EnqueueRequest => DeserializeEnqueueRequest(reader),
                   RequestType.DequeueRequest => DeserializeDequeueRequest(),
                   RequestType.GetCountRequest => DeserializeGetCountRequest(),
                   RequestType.BatchRequest => DeserializeBatchRequest(reader),
                   _ => throw new InvalidEnumArgumentException(nameof(marker), ( int ) marker, typeof(RequestType))
               };
    }

    private static IRequest DeserializeBatchRequest(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        var requests = new List<IRequest>(count);
        for (int i = 0; i < count; i++)
        {
            var request = Deserialize(reader);
            requests.Add(request);
        }

        return new BatchRequest(requests);
    }

    private static IRequest DeserializeGetCountRequest()
    {
        return GetCountRequest.Instance;
    }

    private static IRequest DeserializeDequeueRequest()
    {
        return DequeueRequest.Instance;
    }

    private static EnqueueRequest DeserializeEnqueueRequest(BinaryReader reader)
    {
        var key = reader.ReadInt32();
        var bufferSize = reader.ReadInt32();
        var payload = new byte[bufferSize];
        var read = reader.Read(payload);
        if (read != bufferSize)
        {
            throw new InvalidDataException(
                $"Указанное количество байтов не равно прочитанному. Указанное количество: {bufferSize}. Прочитано: {read}");
        }

        return new EnqueueRequest(key, payload);
    }
}