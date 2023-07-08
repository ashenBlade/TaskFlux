using Raft.StateMachine;

namespace Raft.Core.Commands;

public class NullResponse: IResponse
{
    public static readonly NullResponse Instance = new();
    public void WriteTo(Stream output)
    { }
}