namespace Consensus.CommandQueue.Channel.Tests;

public class LambdaCommandSource
{
    private readonly Action _action;

    public LambdaCommandSource(Action action)
    {
        _action = action;
    }

    public LambdaCommand CreateCommand()
    {
        return new LambdaCommand(_action);
    }
}