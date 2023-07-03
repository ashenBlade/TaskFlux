using Raft.StateMachine;

namespace Raft.Core.Tests;

public class NullResponse: IResponse
{
    public static readonly NullResponse Instance = new NullResponse();
    public void WriteTo(Stream output)
    {
        
    }
}