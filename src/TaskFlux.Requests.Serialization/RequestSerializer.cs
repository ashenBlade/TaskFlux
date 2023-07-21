using System.ComponentModel;
using System.Diagnostics;
using Consensus.Core;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace TaskFlux.Requests.Serialization;

public class RequestSerializer: ISerializer<IRequest>
{
    public static readonly RequestSerializer Instance = new();
    public byte[] Serialize(IRequest command)
    {
        var estimator = new RequestSizeEstimatorVisitor();
        command.Accept(estimator);
        var estimatedSize = estimator.EstimatedSize;
        var buffer = new byte[estimatedSize];
        var memory = new MemoryStream(buffer);
        var writer = new BinaryWriter(memory);
        var visitor = new RequestSerializerVisitor(writer, estimator);
        command.Accept(visitor);
        
        Debug.Assert(estimatedSize == memory.Position, 
            "estimatedSize == memory.Position", 
            "Размер аллоцированного буфера должен быть равен количеству записанных байтов");
        
        return buffer;
    }

    private class RequestSizeEstimatorVisitor : IRequestVisitor
    {
        private const int HeaderSize = sizeof(RequestType) + sizeof(int);
        
        public int EstimatedSize { get; private set; } = HeaderSize;

        public void Visit(GetCountRequest getCountRequest)
        { }

        public void Visit(EnqueueRequest enqueueRequest)
        {
            EstimatedSize += sizeof(int) // Key
                           + sizeof(int) // Payload Length
                           + enqueueRequest.Payload.Length;
        }

        public void Visit(DequeueRequest dequeueRequest)
        { }

        public void Visit(BatchRequest batchRequest)
        {
            if (batchRequest.Requests.Count > 0)
            {
                var resultSize = EstimatedSize + sizeof(int);
                foreach (var request in batchRequest.Requests)
                {
                    EstimatedSize = HeaderSize;
                    request.Accept(this);
                    resultSize += EstimatedSize;
                }
                EstimatedSize = resultSize;
            }
            else
            {
                EstimatedSize += sizeof(int);
            }

        }

        public void Reset()
        {
            EstimatedSize = HeaderSize;
        }
    }
    
    // Лучше будет добавить указание размера изначально
    private class RequestSerializerVisitor : IRequestVisitor
    {
        private readonly BinaryWriter _writer;
        private readonly RequestSizeEstimatorVisitor _estimator;

        public RequestSerializerVisitor(BinaryWriter writer, RequestSizeEstimatorVisitor estimator)
        {
            _writer = writer;
            _estimator = estimator;
        }

        private void WriteHeader(RequestType type, IRequest request)
        {
            _writer.Write((int)type);
            _estimator.Reset();
            request.Accept(_estimator);
            _writer.Write(_estimator.EstimatedSize);
        }
        
        public void Visit(GetCountRequest getCountRequest)
        {
            WriteHeader(RequestType.GetCountRequest, getCountRequest);
        }

        public void Visit(EnqueueRequest enqueueRequest)
        {
            WriteHeader(RequestType.EnqueueRequest, enqueueRequest);
            _writer.Write(enqueueRequest.Key);
            _writer.Write(enqueueRequest.Payload.Length);
            _writer.Write(enqueueRequest.Payload);
        }

        public void Visit(DequeueRequest dequeueRequest)
        {
            WriteHeader(RequestType.DequeueRequest, dequeueRequest);
        }

        public void Visit(BatchRequest batchRequest)
        {
            WriteHeader(RequestType.BatchRequest, batchRequest);
            
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
        _ = reader.ReadInt32();
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