using Consensus.Raft.Commands.Submit;
using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Host.Modules.BinaryRequest;

public class CommandDescriptorBuilderCommandVisitor: IReturningCommandVisitor<CommandDescriptor<Command>>
{
    public static readonly CommandDescriptorBuilderCommandVisitor Instance = new();
    public CommandDescriptor<Command> Visit(EnqueueCommand command)
    {
        return new CommandDescriptor<Command>(command, false);
    }

    public CommandDescriptor<Command> Visit(DequeueCommand command)
    {
        return new CommandDescriptor<Command>(command, false);
    }

    public CommandDescriptor<Command> Visit(CountCommand command)
    {
        return new CommandDescriptor<Command>(command, true);
    }

    public CommandDescriptor<Command> Visit(CreateQueueCommand command)
    {
        return new CommandDescriptor<Command>(command, false);
    }

    public CommandDescriptor<Command> Visit(DeleteQueueCommand command)
    {
        return new CommandDescriptor<Command>(command, false);
    }

    public CommandDescriptor<Command> Visit(ListQueuesCommand command)
    {
        return new CommandDescriptor<Command>(command, true);
    }
}