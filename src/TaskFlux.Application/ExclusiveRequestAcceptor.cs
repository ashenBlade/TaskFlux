using System.Collections.Concurrent;
using Serilog;
using TaskFlux.Consensus;
using TaskFlux.Core.Commands;

namespace TaskFlux.Application;

public class ExclusiveRequestAcceptor : IRequestAcceptor, IDisposable
{
    private readonly IConsensusModule<Command, Response> _module;
    private readonly IApplicationLifetime _lifetime;
    private readonly ILogger _logger;
    private readonly BlockingCollection<UserRequest> _channel = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread _thread;

    /// <summary>
    /// Размер очереди запросов
    /// </summary>
    public int QueueSize
    {
        get
        {
            try
            {
                return _channel.Count;
            }
            catch (ObjectDisposedException)
            {
                return 0;
            }
        }
    }

    public ExclusiveRequestAcceptor(IConsensusModule<Command, Response> module,
        IApplicationLifetime lifetime,
        ILogger logger)
    {
        _module = module;
        _lifetime = lifetime;
        _logger = logger;
        _thread = new Thread(ThreadWorker);
    }

    private record UserRequest(Command Command, CancellationToken Token)
    {
        private readonly TaskCompletionSource<SubmitResponse<Response>> _tcs = new();
        public Task<SubmitResponse<Response>> CommandTask => _tcs.Task;
        public bool IsCancelled => CommandTask.IsCanceled;

        public void SetResult(SubmitResponse<Response> result)
        {
            _tcs.TrySetResult(result);
        }

        public void Cancel()
        {
            _tcs.TrySetCanceled();
        }
    }

    public async Task<SubmitResponse<Response>> AcceptAsync(Command command, CancellationToken token = default)
    {
        var request = new UserRequest(command, token);
        await using var registration = token.Register(static r => ((UserRequest)r!).Cancel(), request);
        _logger.Verbose("Отправляю запрос {@Command} в очередь", command);
        _channel.Add(request, token);
        var result = await request.CommandTask;
        _logger.Debug("Задача команды {@Command} завершилась", command);
        return result;
    }

    private void ThreadWorker()
    {
        CancellationToken token;
        try
        {
            token = _cts.Token;
        }
        catch (ObjectDisposedException disposed)
        {
            _logger.Error(disposed, "Не удалось получить токен отмены: источник токенов закрыт");
            return;
        }

        try
        {
            _logger.Information("Поток обработки команд запущен");
            foreach (var request in _channel.GetConsumingEnumerable(token))
            {
                if (request.IsCancelled)
                {
                    continue;
                }

                if (token.IsCancellationRequested)
                {
                    request.Cancel();
                    continue;
                }

                _logger.Debug("Получена команда {@RequestType}", request.Command);

                try
                {
                    var response = _module.Handle(request.Command, request.Token);
                    request.SetResult(response);
                }
                catch (OperationCanceledException)
                {
                    request.Cancel();
                }

                Metrics.TotalProcessedCommands.Add(1);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            _logger.Fatal(e, "Необработанное исключение поймано во время обработки команды");
            _lifetime.StopAbnormal();
        }

        _logger.Information("Поток обработчик команд завершился");
    }

    public void Start()
    {
        _logger.Information("Запускаю поток обработки пользовательских запросов");
        _thread.Start();
    }

    public void Stop()
    {
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public void Dispose()
    {
        _cts.Dispose();
        _channel.Dispose();
        if (_thread.ThreadState is not ThreadState.Unstarted)
        {
            _thread.Join();
        }
    }
}