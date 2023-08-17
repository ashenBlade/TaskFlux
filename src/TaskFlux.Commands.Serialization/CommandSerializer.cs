using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using JobQueue.Core;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Serialization.Helpers;

namespace TaskFlux.Commands.Serialization;

public class CommandSerializer
{
    public static readonly CommandSerializer Instance = new();
    public byte[] Serialize(Command command)
    {
        var visitor = new CommandSerializerVisitor();
        command.Accept(visitor);
        return visitor.Result;
    }

    private class CommandSerializerVisitor : ICommandVisitor
    {
        private byte[]? _result;
        public byte[] Result => _result ?? throw new ArgumentNullException(nameof(_result), "Сериализованное значениеу не выставлено");
        
        public void Visit(EnqueueCommand command)
        {
            var queueNameSize = MemoryBinaryWriter.EstimateResultStringSize(command.Queue);
            var estimatedSize = sizeof(CommandType)     // Маркер
                              + queueNameSize           // Очередь
                              + sizeof(long)            // Ключ
                              + sizeof(int)             // Длина тела
                              + command.Payload.Length; // Тело
            
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte) CommandType.Enqueue);
            writer.Write(command.Queue);
            writer.Write(command.Key);
            writer.WriteBuffer(command.Payload);
            _result = buffer;
        }

        public void Visit(DequeueCommand command)
        {
            var estimatedQueueNameSize = MemoryBinaryWriter.EstimateResultStringSize(command.Queue);
            var estimatedSize = sizeof(CommandType)     // Маркер
                              + estimatedQueueNameSize; // Очередь
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)CommandType.Dequeue);
            writer.Write(command.Queue);
            _result = buffer;
        }

        public void Visit(CountCommand command)
        {
            var estimatedQueueNameSize = MemoryBinaryWriter.EstimateResultStringSize(command.Queue);
            var estimatedSize = sizeof(CommandType)     // Маркер
                              + estimatedQueueNameSize; // Очередь
            var buffer = new byte[estimatedSize];
            var writer = new MemoryBinaryWriter(buffer);
            writer.Write((byte)CommandType.Count);
            writer.Write(command.Queue);
            _result = buffer;
        }
    }
    
    /// <summary>
    /// Десериализовать переданный массив байтов в соответствующую команду
    /// </summary>
    /// <param name="payload">Сериализованная команда</param>
    /// <returns>Команда, которая была сериализована</returns>
    /// <exception cref="InvalidQueueNameException">Сериализованное название очереди было в неправильном формате</exception>
    /// <exception cref="ArgumentNullException">Переданный массив - <c>null</c></exception>
    /// <exception cref="SerializationException">Во время десериализации произошла ошибка</exception>
    public Command Deserialize(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Length == 0)
        {
            throw new SerializationException("Переданный буффер был пуст");
        }
        
        var reader = new ArrayBinaryReader(payload);
        var marker = (CommandType) reader.ReadByte();
        try
        {
            return marker switch
                   {
                       CommandType.Count   => DeserializeCountCommand(reader),
                       CommandType.Dequeue => DeserializeDequeueCommand(reader),
                       CommandType.Enqueue => DeserializeEnqueueCommand(reader),
                   };
        }
        // Ушли за границы буфера - передана не полная информация
        // (например, только 4 байта, для long (8 байт надо)).
        // Многие такие моменты уже переделаны под SerializationException, но лучше перестраховаться
        catch (IndexOutOfRangeException e)
        {
            throw new SerializationException($"Ошибка во время десерилизации команды {marker}", e);
        }
        // Такой тип перечисления не найден - байт неправильный
        catch (SwitchExpressionException)
        {
            throw new SerializationException($"Неизвестный байт маркера команды: {( byte ) marker}");
        }
    }

    private DequeueCommand DeserializeDequeueCommand(ArrayBinaryReader reader)
    {
        var name = reader.ReadString();
        return new DequeueCommand(QueueName.Parse(name));
    }

    private CountCommand DeserializeCountCommand(ArrayBinaryReader reader)
    {
        var queue = reader.ReadString();
        return new CountCommand(QueueName.Parse(queue));
    }

    private EnqueueCommand DeserializeEnqueueCommand(ArrayBinaryReader reader)
    {
        var queue = reader.ReadString();
        var key = reader.ReadInt64();
        var buffer = reader.ReadBuffer();
        return new EnqueueCommand(key, buffer, QueueName.Parse(queue));
    }
}