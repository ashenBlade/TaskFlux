using Consensus.Raft;
using Consensus.Raft.Commands.Submit;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Host.Modules;

namespace TaskFlux.Host.RequestAcceptor;

public class ExclusiveRequestAcceptor : IRequestAcceptor, IDisposable
{
    private readonly IConsensusModule<Command, Result> _module;
    private readonly ILogger _logger;
    private readonly BlockingChannel<UserRequest> _channel = new();
    private readonly CancellationTokenSource _cts = new();
    private CancellationTokenRegistration? _tokenRegistration = null;
    private Thread? _thread;

    public ExclusiveRequestAcceptor(IConsensusModule<Command, Result> module, ILogger logger)
    {
        _module = module;
        _logger = logger;
    }

    private record UserRequest(Command Command, CancellationToken Token)
    {
        private readonly TaskCompletionSource<SubmitResponse<Result>> _tcs = new();
        public Task<SubmitResponse<Result>> CommandTask => _tcs.Task;
        public bool IsCancelled => CommandTask.IsCanceled;

        public void SetResult(SubmitResponse<Result> result)
        {
            _tcs.TrySetResult(result);
        }

        public void Cancel()
        {
            _tcs.TrySetCanceled();
        }

        public CommandDescriptor<Command> GetDescriptor()
        {
            return new CommandDescriptor<Command>(Command, Command.IsReadOnly);
        }
    }

    public async Task<SubmitResponse<Result>> AcceptAsync(Command command, CancellationToken token = default)
    {
        var request = new UserRequest(command, token);
        var registration = new CancellationTokenRegistration();
        try
        {
            if (token.CanBeCanceled)
            {
                registration = token.Register(r => ( ( UserRequest ) r! ).Cancel(), request);
            }

            _channel.Write(request);
            return await request.CommandTask;
        }
        finally
        {
            await registration.DisposeAsync();
        }
    }

    private void ThreadWorker()
    {
        try
        {
            foreach (var request in _channel.ReadAll(_cts.Token))
            {
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