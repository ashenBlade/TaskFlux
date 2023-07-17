namespace Consensus.CommandQueue.Channel.Tests;

public class LambdaCommand: ICommand
{
    private readonly Action _action;

    public LambdaCommand(Action action)
    {
        _action = action;
    }
    public void Execute()
    {
        _action();
    }
}