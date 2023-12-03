using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Serilog;
using TaskFlux.Core;
using TaskFlux.Network.Client;

namespace TaskFlux.Host.Modules.SocketRequest;

internal class ClientRequestProcessor
{
    private readonly TcpClient _client;
    private readonly IOptionsMonitor<SocketRequestModuleOptions> _options;
    private readonly IApplicationInfo _applicationInfo;
    private readonly IRequestAcceptor _requestAcceptor;
    private IClusterInfo ClusterInfo { get; }
    private readonly ILogger _logger;

    public ClientRequestProcessor(TcpClient client,
                                  IRequestAcceptor requestAcceptor,
                                  IOptionsMonitor<SocketRequestModuleOptions> options,
                                  IApplicationInfo applicationInfo,
                                  IClusterInfo clusterInfo,
                                  ILogger logger)
    {
        _client = client;
        _requestAcceptor = requestAcceptor;
        _options = options;
        _applicationInfo = applicationInfo;
        ClusterInfo = clusterInfo;
        _logger = logger;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        // 1. Авторизуем клиента
        // 2. Настраиваем клиента
        // 3. Обрабатываем его запросы (Enqueue, Dequeue, Create, ...)
        _logger.Information("Начинаю обработку клиента");
        await using var stream = _client.GetStream();
        var client = new TaskFluxClient(stream);
        try
        {
            if (await AcceptSetupClientAsync(client, token))
            {
                await ProcessClientForeverAsync(client, token);
            }
        }
        catch (EndOfStreamException)
        {
            // Клиент разорвал соединение
        }
    }

    /// <summary>
    /// Принять и настроить клиента
    /// </summary>
    /// <returns><c>true</c> - клиент настроен успешно, <c>false</c> - иначе</returns>
    private async Task<bool> AcceptSetupClientAsync(TaskFluxClient client, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    private async Task ProcessClientForeverAsync(TaskFluxClient client, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}