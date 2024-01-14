using System.Collections.Concurrent;
using Consensus.Core.Submit;
using Consensus.Raft;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Transport.Common;

namespace TaskFlux.Host.RequestAcceptor;

public class ExclusiveRequestAcceptor : IRequestAcceptor, IDisposable
{
    private readonly IRaftConsensusModule<Command, Response> _module;
    private readonly ILogger _logger;
    private readonly BlockingCollection<UserRequest> _channel = new();
    private readonly CancellationTokenSource _cts = new();
    private Thread? _thread;

    public ExclusiveRequestAcceptor(IRaftConsensusModule<Command, Response> module, ILogger logger)
    {
        _module = module;
        _logger = logger;
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
                    request.Cancel();
                }
            }

            _logger.Information("Заканчиваю читать запросы пользователей");
        }
        catch (Exception e)
        {
            _logger.Fatal(e, "Ошибка во время обработки пользовательских запросов");
            throw;
        }
    }

    public void Start()
    {
        _thread = new Thread(ThreadWorker);
        _logger.Information("Запускаю поток обработки пользовательских запросов");
        _thread.Start();
    }

    public void Stop()
    {
        _cts.Cancel();
    }

    public void Dispose()
    {
        _cts.Dispose();
        _channel.Dispose();
        _thread?.Join();
    }
}