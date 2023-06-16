using System.Threading.Channels;
using Raft.Core.Commands;

namespace Raft.CommandQueue;

public class ChannelCommandQueue: ICommandQueue, IDisposable
{
    private readonly Channel<CommandChannelEntry> _channel = Channel.CreateUnbounded<CommandChannelEntry>(new UnboundedChannelOptions()
    {
        SingleReader = true,
        SingleWriter = false,
        // Не ставить в true иначе при пустой очереди получим дедлок
        // 
        AllowSynchronousContinuations = false
    });

    private IDisposable BeginScope()
    {
        var scope = new CommandChannelEntry(_channel);
        try
        {
            scope.Enqueue();
            scope.WaitReady();
            return scope;
        }
        catch (Exception)
        {
            scope.Cancel();
            throw;
        }
    }
    
    public void Enqueue(ICommand command)
    {
        using var _ = BeginScope();
        command.Execute();
    }

    public T Enqueue<T>(ICommand<T> command)
    {
        using var _ = BeginScope();
        return command.Execute();
    }

    /// <summary>
    /// Запустить обработку событий этого 
    /// </summary>
    /// <param name="token">Токен отмены</param>
    public async Task RunAsync(CancellationToken token)
    {
        await Task.Yield();
        try
        {
            await foreach (var entry in _channel.Reader.ReadAllAsync(token))
            {
                try
                {
                    entry.SetReady();
                    entry.WaitComplete();
                }
                catch (Exception)
                {
                    //
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        _channel.Writer.Complete();
    }

    public virtual void Dispose()
    {
        _channel.Writer.Complete();
    }
}