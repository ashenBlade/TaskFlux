using Consensus.Core.Commands.Submit;
using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;

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
}