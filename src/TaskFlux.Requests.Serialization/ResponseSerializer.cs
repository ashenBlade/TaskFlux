using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using Consensus.Core;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Error;
using TaskFlux.Requests.Requests.JobQueue.GetCount;

namespace TaskFlux.Requests.Serialization;

public class ResponseSerializer: ISerializer<IResponse>
{
    public static readonly ResponseSerializer Instance = new();
    public byte[] Serialize(IResponse command)
    {
        var estimator = new ResponseSizeEstimatorVisitor();
        command.Accept(estimator);
        var totalPacketSize = estimator.EstimatedSize;
        var buffer = new byte[totalPacketSize];
        var memoryStream = new MemoryStream(buffer);
        var visitor = new ResponseSerializerVisitor(new BinaryWriter(memoryStream), estimator);
        command.Accept(visitor);
        
        Debug.Assert(memoryStream.Position == totalPacketSize, 
            "memoryStream.Position == totalPacketSize",
            "Размер выделенной памяти под массив должен быть равен количеству записанных данных");
        
        return buffer;
    }

    private class ResponseSizeEstimatorVisitor : IResponseVisitor
    {
        private const int BaseEstimatedSize = sizeof(ResponseType) // Marker 
                                            + sizeof(int);         // Size
        public int EstimatedSize { get; set; } = BaseEstimatedSize;
        public void Visit(GetCountResponse response)
        {
            EstimatedSize += sizeof(int);
        }

        public void Visit(EnqueueResponse response)
        {
            EstimatedSize += sizeof(byte);
        }

        public void Visit(DequeueResponse response)
        {
            int additional;
            if (response.Success)
            {
                additional = sizeof(byte) // Success
                            +sizeof(int)  // Key 
                           + sizeof(int)  // Payload Length
                           + response.Payload.Length;
            }
            else
            {
                additional = sizeof(byte); // Success
            }
            
            EstimatedSize += additional;
        }

        // https://stackoverflow.com/a/49780224
        private static int Get7BitEncodedIntResultLength(int value)
        {
            return value switch
                   {
                       < 0         => 5,
                       < 128       => 1,
                       < 16384     => 2,
                       < 2097152   => 3,
                       < 268435456 => 4,
                       _           => 5
                   };
        }
        
        public void Visit(ErrorResponse response)
        {
            var messageSizeBytes = Encoding.UTF8.GetByteCount(response.Message);
            EstimatedSize += Get7BitEncodedIntResultLength(messageSizeBytes)
                           + messageSizeBytes;
        }

        public void Visit(BatchResponse batchResponse)
        {
            if (batchResponse.Responses.Count == 0)
            {
                EstimatedSize += sizeof(int);
                return;
            }

            var resultSize = EstimatedSize + sizeof(int);
            
            foreach (var response in batchResponse.Responses)
            {
                EstimatedSize = BaseEstimatedSize;
                response.Accept(this);
                resultSize += EstimatedSize;
            }

            EstimatedSize = resultSize;
        }

        public void Reset()
        {
            EstimatedSize = BaseEstimatedSize;
        }
    }

    private class ResponseSerializerVisitor : IResponseVisitor
    {
        private readonly BinaryWriter _writer;
        private readonly ResponseSizeEstimatorVisitor _estimator;

        public ResponseSerializerVisitor(BinaryWriter writer, ResponseSizeEstimatorVisitor estimator)
        {
            _writer = writer;
            _estimator = estimator;
        }

        private void WriteHeader(ResponseType type, IResponse response)
        {
            _writer.Write((int)type);
            _estimator.Reset();
            response.Accept(_estimator);
            _writer.Write(_estimator.EstimatedSize);
        }
        
        public void Visit(GetCountResponse response)
        {
            WriteHeader(ResponseType.GetCountResponse, response);
            _writer.Write(response.Count);
        }

        public void Visit(EnqueueResponse response)
        {
            WriteHeader(ResponseType.EnqueueResponse, response);
            _writer.Write(response.Success);
        }

        public void Visit(DequeueResponse response)
        {
            WriteHeader(ResponseType.DequeueResponse, response);
            var success = response.Success;
            _writer.Write(success);
            if (success)
            {
                _writer.Write(response.Key);
                _writer.Write(response.Payload.Length);
                _writer.Write(response.Payload);
            }
        }

        public void Visit(ErrorResponse response)
        {
            WriteHeader(ResponseType.ErrorResponse, response);
            _writer.Write(response.Message);
        }

        public void Visit(BatchResponse batchResponse)
        {
            _writer.Write((int)ResponseType.BatchResponse);
            _estimator.Reset();
            batchResponse.Accept(_estimator);
            _writer.Write(_estimator.EstimatedSize);

            var responses = batchResponse.Responses;
            _writer.Write(responses.Count);
            if (responses.Count > 0)
            {
                foreach (var response in responses)
                {
                    response.Accept(this);
                }
            }
        }
    }
    
    public IResponse Deserialize(byte[] payload)
    {
        var memory = new MemoryStream(payload);
        var reader = new BinaryReader(memory);
        return Deserialize(reader);
    }

    private IResponse Deserialize(BinaryReader reader)
    {
        var marker = ( ResponseType ) reader.ReadInt32();
        _ = reader.ReadInt32(); // Length
        return marker switch
               {
                   ResponseType.EnqueueResponse  => DeserializeEnqueueResponse(reader),
                   ResponseType.DequeueResponse  => DeserializeDequeueResponse(reader),
                   ResponseType.GetCountResponse => DeserializeGetCountResponse(reader),
                   ResponseType.ErrorResponse    => DeserializeErrorResponse(reader),
                   ResponseType.BatchResponse    => DeserializeBatchResponse(reader),
                   _ => throw new InvalidEnumArgumentException(nameof(marker), (int)marker, typeof(ResponseType))
               };
    }

    private BatchResponse DeserializeBatchResponse(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count == 0)
        {
            return new BatchResponse(Array.Empty<IResponse>());
        }
        
        var responses = new List<IResponse>(count);
        for (int i = 0; i < count; i++)
        {
            var response = Deserialize(reader);
            responses.Add(response);
        }

        return new BatchResponse(responses);
    }

    private static ErrorResponse DeserializeErrorResponse(BinaryReader reader)
    {
        var message = reader.ReadString();
        return new ErrorResponse(message);
    }

    private static GetCountResponse DeserializeGetCountResponse(BinaryReader reader)
    {
        var count = reader.ReadInt32();
        return count == 0
                   ? GetCountResponse.Empty
                   : new GetCountResponse(count);
    }

    private static DequeueResponse DeserializeDequeueResponse(BinaryReader reader)
    {
        var success = reader.ReadBoolean();
        if (success)
        {
            var key = reader.ReadInt32();
            var bufferSize = reader.ReadInt32();
            var payload = new byte[bufferSize];
            var read = reader.Read(payload);
            if (read != bufferSize)
            {
                throw new InvalidDataException(
                    $"Прочитанное количество байтов не равно прочитанному. Прочитано: {read}. Указано: {bufferSize}");
            }

            return new DequeueResponse(true, key, payload);
        }
        
        return DequeueResponse.Empty;
    }

    private static IResponse DeserializeEnqueueResponse(BinaryReader reader)
    {
        var success = reader.ReadBoolean();
        return new EnqueueResponse(success);
    }

}