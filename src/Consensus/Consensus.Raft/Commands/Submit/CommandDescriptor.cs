namespace Consensus.Raft.Commands.Submit;

public struct CommandDescriptor<TCommand>
{
    public TCommand Command { get; }
    public bool IsReadonly { get; }

    public CommandDescriptor(TCommand command, bool isReadonly)
    {
        Command = command;
        IsReadonly = isReadonly;
    }
}

public static class CommandDescriptor
{
    public static CommandDescriptor<TCommand> Create<TCommand>(TCommand command, bool isReadonly = false) =>
        new(command, isReadonly);
}