namespace Consensus.Core.Tests;

public class NullResponse
{
    public static readonly NullResponse Instance = new NullResponse();
    public void WriteTo(Stream output)
    {
        
    }
}