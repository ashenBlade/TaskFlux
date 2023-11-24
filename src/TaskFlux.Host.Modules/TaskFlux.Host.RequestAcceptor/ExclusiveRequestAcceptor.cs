using Consensus.Core;
using Consensus.Raft;
using Consensus.Raft.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Host.Modules;

namespace TaskFlux.Host.RequestAcceptor;

public class ExclusiveRequestAcceptor : IRequestAcceptor, IDisposable
{
    private readonly IRaftConsensusModule<Command, Response> _module;
    private readonly ILogger _logger;
    private readonly BlockingChannel<UserRequest> _channel = new();
    private readonly CancellationTokenSource _cts = new();
    private CancellationTokenRegistration? _tokenRegistration = null;
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

        public CommandDescriptor<Command> GetDescriptor()
        {
            return new CommandDescriptor<Command>(Command, !Command.IsReadOnly);
        }
    }

    public async Task<SubmitResponse<Response>> AcceptAsync(Command command, CancellationToken token = default)
    {
        var request = new UserRequest(command, token);
        var registration = new CancellationTokenRegistration();
        try
        {
            if (token.CanBeCanceled)
            {
                registration = token.Register(static r => ( ( UserRequest ) r! ).Cancel(), request);
            }

            _logger.Debug("Отправляю запрос в очередь");
            _channel.Write(request);
            var result = await request.CommandTask;
            _logger.Debug("Команда выполнилась");
            return result;
        }
        finally
        {
            await registration.DisposeAsync();
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
            foreach (var request in _channel.ReadAll(token))
            {
                _logger.Debug("Получен запрос: {@RequestType}", request.Command);
                if (request.IsCancelled)
                {
                    _logger.Debug("Запрос {RequestType} был отменен пока лежал в очереди", request.Command.Type);
                    continue;
                }

                try
                {
                    var response = _module.Handle(new SubmitRequest<Command>(request.GetDescriptor()), request.Token);
                    request.SetResult(response);
                }
                catch (OperationCanceledException o)
                {
                    _logger.Debug(o, "Запрос {RequestType} был отменен во время выполнения", request.Command.Type);
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

    public void Start(CancellationToken token)
    {
        _tokenRegistration = token.Register(() => _cts.Cancel());
        _thread = new Thread(ThreadWorker);
        _logger.Information("Запускаю поток обработки пользовательских запросов");
        _thread.Start();
    }

    public void Dispose()
    {
        try
        {
            _cts.Cancel();
            _cts.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }

        _tokenRegistration?.Dispose();
        _channel.Dispose();
        _thread?.Join();
    }
}