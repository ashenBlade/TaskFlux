using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Models;
using TaskFlux.Network.Packets.Commands;
using TaskFlux.PriorityQueue;

namespace TaskFlux.Network.Client;

public static class CommandMapper
{
    public static Command Map(NetworkCommand command)
    {
        return command.Accept(ConverterCommandVisitor.Instance);
    }

    private class ConverterCommandVisitor : INetworkCommandVisitor<Command>
    {
        public static readonly ConverterCommandVisitor Instance = new();

        #region Network -> Command

        public Command Visit(CountNetworkCommand command)
        {
            return new CountCommand(QueueName.Parse(command.QueueName));
        }

        public Command Visit(CreateQueueNetworkCommand command)
        {
            var code = command.Code switch
                       {
                           PriorityQueueCodes.Heap       => PriorityQueueCode.Heap4Arity,
                           PriorityQueueCodes.Default    => PriorityQueueCode.Heap4Arity,
                           PriorityQueueCodes.QueueArray => PriorityQueueCode.QueueArray,
                           _ => throw new ArgumentOutOfRangeException(nameof(command.Code), command.Code,
                                    "Неизвестный код очереди")
                       };
            return new CreateQueueCommand(QueueName.Parse(command.QueueName), code, command.MaxQueueSize,
                command.MaxMessageSize,
                command.PriorityRange);
        }

        public Command Visit(DeleteQueueNetworkCommand command)
        {
            return new DeleteQueueCommand(QueueName.Parse(command.QueueName));
        }

        public Command Visit(ListQueuesNetworkCommand command)
        {
            return ListQueuesCommand.Instance;
        }

        public Command Visit(EnqueueNetworkCommand command)
        {
            return new EnqueueCommand(command.Key, command.Message, QueueName.Parse(command.QueueName));
        }

        public Command Visit(DequeueNetworkCommand command)
        {
            return new DequeueCommand(QueueName.Parse(command.QueueName));
        }

        #endregion
    }
}