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
        CancellationTokenRegistration? registration = token.CanBeCanceled
                                                          ? token.Register(static r => ( ( UserRequest ) r! ).Cancel(),
                                                              request)
                                                          : null;
        try
        {
            _logger.Debug("Отправляю запрос в очередь");
            _channel.Add(request, token);
            var result = await request.CommandTask;
            _logger.Debug("Команда выполнилась");
            return result;
        }
        finally
        {
            if (registration is { } r)
            {
                await r.DisposeAsync();
            }
        }
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
            _logger.Information("Начинаю читать запросы от пользователей");
            foreach (var request in _channel.GetConsumingEnumerable(token))
            {
                if (request.IsCancelled)
                {
                    continue;
                }

                _logger.Debug("Получен запрос: {@RequestType}", request.Command);

                try
                {
                    var response = _module.Handle(request.Command, request.Token);
                    request.SetResult(response);
                }
                catch (OperationCanceledException)
                {
                    _logger.Information("Токен сработал. Заканчиваю работу");
                    request.Cancel();
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            _logger.Fatal(e, "Ошибка во время обработки пользовательских запросов");
            _lifetime.StopAbnormal();
        }

        _logger.Information("Обработчик пользовательских запросов остановлен");
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
        _thread.Join();
    }
}