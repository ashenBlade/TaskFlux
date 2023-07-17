using Consensus.StateMachine;

namespace Consensus.Core.Commands;

public class NullResponse: IResponse
{
    public static readonly NullResponse Instance = new();
    public void WriteTo(Stream output)
    { }
}