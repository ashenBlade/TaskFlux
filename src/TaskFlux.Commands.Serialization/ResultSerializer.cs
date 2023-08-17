using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Commands.Serialization;

public class ResultSerializer
{
    public static readonly ResultSerializer Instance = new();
    public byte[] Serialize(Result result)
    {
        var visitor = new SerializerResultVisitor();
        result.Accept(visitor);
        return visitor.Buffer;
    }

    private class SerializerResultVisitor : IResultVisitor
    {
        private byte[]? _buffer;
        public byte[] Buffer => _buffer ?? throw new ArgumentNullException(nameof(_buffer), "Сериализованное значениеу не выставлено");

        public void Visit(EnqueueResult result)
        {
            var estimatedSize = sizeof(ResultType)
                              + sizeof(bool);
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)ResultType.Enqueue);
            writer.Write(result.Success);
            _buffer = buffer;
        }

        public void Visit(DequeueResult result)
        {
            if (result.TryGetResult(out var key, out var payload))
            {
                var estimatedSize = sizeof(ResultType)     // Маркер
                                  + sizeof(bool)           // Успех
                                  + sizeof(long)           // Ключ
                                  + sizeof(int)            // Размер данных
                                  + result.Payload.Length; // Данные
                var buffer = new byte[estimatedSize];
                var writer = new MemoryBinaryWriter(buffer);
                writer.Write((byte)ResultType.Dequeue);
                writer.Write(true);
                writer.Write(key);
                writer.WriteBuffer(payload);
                _buffer = buffer;
            }
            else
            {
                _buffer = new byte[]
                {
                    ( byte ) ResultType.Dequeue, 
                    0 // Успех: false
                };
            }
        }

        public void Visit(CountResult result)
        {
            var estimatedSize = sizeof(ResultType) // Маркер
                              + sizeof(int);       // Количество
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)ResultType.Count);
            writer.Write(result.Count);
            _buffer = buffer;
        }

        public void Visit(ErrorResult result)
        {
            var estimatedMessageSize = MemoryBinaryWriter.EstimateResultStringSize(result.Message);
            var estimatedSize = sizeof(ResultType)    // Маркер
                              + sizeof(ErrorType)     // Тип ошибки
                              + estimatedMessageSize; // Сообщение 

            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            
            writer.Write((byte)ResultType.Error);
            writer.Write((byte)result.ErrorType);
            writer.Write(result.Message);

            _buffer = buffer;
        }
    }

    public Result Deserialize(byte[] payload)
    {
        var reader = new ArrayBinaryReader(payload);
        var marker = (ResultType) reader.ReadByte();
        return marker switch
               {
                   ResultType.Count   => DeserializeCountResult(reader),
                   ResultType.Dequeue => DeserializeDequeueResult(reader),
                   ResultType.Enqueue => DeserializeEnqueueResult(reader),
                   ResultType.Error   => DeserializeErrorResult(reader),
               };
    }

    private ErrorResult DeserializeErrorResult(ArrayBinaryReader reader)
    {
        var subtype = ( ErrorType ) reader.ReadByte();
        var message = reader.ReadString();
        return new ErrorResult(subtype, message);
    }

    private CountResult DeserializeCountResult(ArrayBinaryReader reader)
    {
        var count = reader.ReadInt32();
        if (count == 0)
        {
            return CountResult.Empty;
        }
        
        return new CountResult(count);
    }

    private EnqueueResult DeserializeEnqueueResult(ArrayBinaryReader reader)
    {
        var success = reader.ReadBoolean();
        return success
                   ? EnqueueResult.Ok
                   : EnqueueResult.Full;
    }

    private DequeueResult DeserializeDequeueResult(ArrayBinaryReader reader)
    {
        var hasValue = reader.ReadBoolean();
        if (!hasValue)
        {
            return DequeueResult.Empty;
        }

        var key = reader.ReadInt64();
        var payload = reader.ReadBuffer();
        return DequeueResult.Create(key, payload);
    }
}