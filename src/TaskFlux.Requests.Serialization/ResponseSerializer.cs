using System.ComponentModel;
using Consensus.Core;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Commands.JobQueue.GetCount;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Error;

namespace TaskFlux.Requests.Serialization;

public class ResponseSerializer: ISerializer<IResponse>
{
    public static readonly ResponseSerializer Instance = new();
    public byte[] Serialize(IResponse command)
    {
        var memory = new MemoryStream();
        var writer = new BinaryWriter(memory);
        var visitor = new ResponseSerializerVisitor(writer);
        command.Accept(visitor);
        return memory.ToArray();
    }

    private class ResponseSerializerVisitor : IResponseVisitor
    {
        private readonly BinaryWriter _writer;

        public ResponseSerializerVisitor(BinaryWriter writer)
        {
            _writer = writer;
        }

        private void WriteMarker(ResponseType type)
        {
            _writer.Write((int)type);
        }
        
        public void Visit(GetCountResponse getCountResponse)
        {
            WriteMarker(ResponseType.GetCountResponse);
            _writer.Write(getCountResponse.Count);
        }

        public void Visit(EnqueueResponse enqueueResponse)
        {
            WriteMarker(ResponseType.EnqueueResponse);
            _writer.Write(enqueueResponse.Success);
        }

        public void Visit(DequeueResponse dequeueResponse)
        {
            WriteMarker(ResponseType.DequeueResponse);
            var success = dequeueResponse.Success;
            _writer.Write(success);
            if (success)
            {
                _writer.Write(dequeueResponse.Key);
                _writer.Write(dequeueResponse.Payload.Length);
                _writer.Write(dequeueResponse.Payload);
            }
        }

        public void Visit(ErrorResponse errorResponse)
        {
            WriteMarker(ResponseType.ErrorResponse);
            _writer.Write(errorResponse.Message);
        }

        public void Visit(BatchResponse batchResponse)
        {
            WriteMarker(ResponseType.BatchResponse);
            _writer.Write(batchResponse.Responses.Count);
            foreach (var response in batchResponse.Responses)
            {
                response.Accept(this);
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