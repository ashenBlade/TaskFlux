using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.CreateQueue.ImplementationDetails;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Models;
using TaskFlux.Models.Exceptions;
using TaskFlux.Network.Commands;

namespace TaskFlux.Host.Modules.SocketRequest.Mapping;

public static class CommandMapper
{
    /// <summary>
    /// Конвертировать полученную по сети команду во внутреннюю команду
    /// </summary>
    /// <param name="command">Команда полученная по сети</param>
    /// <returns>Внутренняя команда</returns>
    /// <exception cref="MappingException">Во время маппинга произошли ошибки валидации входных параметров от клиента. Тип возникшей ошибки передается в свойстве <see cref="MappingException.ErrorCode"/></exception>
    public static Command Map(NetworkCommand command)
    {
        try
        {
            return command.Accept(ConverterCommandVisitor.Instance);
        }
        catch (InvalidQueueNameException)
        {
            throw new MappingException(ErrorType.InvalidQueueName);
        }
        catch (InvalidMaxPayloadSizeException)
        {
            throw new MappingException(ErrorType.InvalidMaxPayloadSize);
        }
        catch (InvalidPriorityRangeException)
        {
            throw new MappingException(ErrorType.InvalidPriorityRange);
        }
        catch (InvalidMaxQueueSizeException)
        {
            throw new MappingException(ErrorType.InvalidMaxQueueSize);
        }
    }

    private class ConverterCommandVisitor : INetworkCommandVisitor<Command>
    {
        public static readonly ConverterCommandVisitor Instance = new();

        public Command Visit(CountNetworkCommand command)
        {
            return new CountCommand(QueueName.Parse(command.QueueName));
        }

        public Command Visit(CreateQueueNetworkCommand command)
        {
            QueueImplementationDetails details;
            switch (command.Code)
            {
                case NetworkPriorityQueueCodes.Heap:
                    details = new HeapQueueDetails()
                    {
                        PriorityRange = command.PriorityRange,
                        MaxPayloadSize = command.MaxPayloadSize,
                        MaxQueueSize = command.MaxQueueSize,
                    };
                    break;
                case NetworkPriorityQueueCodes.QueueArray:
                    if (command.TryGetPriorityRange(out var min, out var max))
                    {
                        details = new QueueArrayQueueDetails(( min, max ))
                        {
                            MaxPayloadSize = command.MaxPayloadSize, MaxQueueSize = command.MaxQueueSize,
                        };
                    }
                    else
                    {
                        throw new MappingException(ErrorType.PriorityRangeNotSpecified);
                    }

                    break;
                default:
                    throw new MappingException(ErrorType.UnknownPriorityQueueCode);
            }

            return new CreateQueueCommand(queue: QueueName.Parse(command.QueueName),
                details: details);
        }

        public Command Visit(DeleteQueueNetworkCommand command)
        {
            return new DeleteQueueCommand(queue: QueueName.Parse(command.QueueName));
        }

        public Command Visit(ListQueuesNetworkCommand command)
        {
            return ListQueuesCommand.Instance;
        }

        public Command Visit(EnqueueNetworkCommand command)
        {
            return new EnqueueCommand(key: command.Key,
                message: command.Message,
                queue: QueueName.Parse(command.QueueName));
        }

        public Command Visit(DequeueNetworkCommand command)
        {
            return new DequeueRecordCommand(queue: QueueName.Parse(command.QueueName),
                permanent: false); // Команда не должна быть перманентной, т.к. должно будут Ack/Nack пакеты дополнительные
        }
    }
}