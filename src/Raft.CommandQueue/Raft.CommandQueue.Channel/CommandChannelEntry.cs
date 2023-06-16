using System.Threading.Channels;

namespace Raft.CommandQueue;

internal record CommandChannelEntry: IDisposable
{
    private readonly Channel<CommandChannelEntry> _channel;
    public TaskCompletionSource AvailableTaskSource { get; } = new();
    public TaskCompletionSource CompletionTaskSource { get; } = new();

    public CommandChannelEntry(Channel<CommandChannelEntry> channel)
    {
        _channel = channel;
    }

    public void Enqueue()
    {
        // При использовании Unbounded всегда возвращает true
        _channel.Writer.TryWrite(this);
    }

    public void WaitReady()
    {
        AvailableTaskSource.Task.Wait();
    }

    public void Cancel()
    {
        try
        {
            AvailableTaskSource.SetCanceled();
        }
        catch (InvalidOperationException)
        { }
            
            
        try
        {
            CompletionTaskSource.SetCanceled();
        }
        catch (InvalidOperationException)
        { }
    }

    public void Dispose()
    {
        CompletionTaskSource.SetResult();
        CompletionTaskSource.Task.Dispose();
            
        AvailableTaskSource.Task.Dispose();
    }

    public void SetReady()
    {
        AvailableTaskSource.SetResult();
    }

    public void WaitComplete()
    {
        CompletionTaskSource.Task.Wait();
    }
}