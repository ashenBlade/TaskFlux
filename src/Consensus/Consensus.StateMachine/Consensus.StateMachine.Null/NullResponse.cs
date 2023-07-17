namespace Consensus.StateMachine.Null;

public class NullResponse: IResponse
{
    public static readonly NullResponse Instance = new();
    
    public void WriteTo(Stream output)
    { }
}