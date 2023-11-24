namespace Consensus.Core;

public struct CommandDescriptor<TCommand>
{
    public TCommand Command { get; }
    public bool ShouldReplicate { get; }

    public CommandDescriptor(TCommand command, bool shouldReplicate)
    {
        Command = command;
        ShouldReplicate = shouldReplicate;
    }
}