namespace Consensus.StateMachine.Null;

public class NullResponse
{
    public static readonly NullResponse Instance = new();
    
    public void WriteTo(Stream output)
    { }
}