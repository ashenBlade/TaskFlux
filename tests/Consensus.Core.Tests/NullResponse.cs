using Consensus.StateMachine;

namespace Consensus.Core.Tests;

public class NullResponse: IResponse
{
    public static readonly NullResponse Instance = new NullResponse();
    public void WriteTo(Stream output)
    {
        
    }
}