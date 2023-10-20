namespace Consensus.Raft.Commands;

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