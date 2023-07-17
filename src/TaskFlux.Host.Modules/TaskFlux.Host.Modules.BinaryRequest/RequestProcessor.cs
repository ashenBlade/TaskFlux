using System.Net.Sockets;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;

namespace TaskFlux.Host.Modules.BinaryRequest;

internal class RequestProcessor
{
    private const int DefaultBufferSize = 256;
    private readonly TcpClient _client;
    private readonly IConsensusModule _consensusModule;
    private readonly ILogger _logger;

    public RequestProcessor(TcpClient client, IConsensusModule consensusModule, ILogger logger)
    {
        _client = client;
        _consensusModule = consensusModule;
        _logger = logger;
    }

    public async Task ProcessAsync(CancellationToken token)
    {
        _logger.Debug("Начинаю обработку полученного запроса");
        var requestBuffer = new List<byte>();
        var buffer = new byte[DefaultBufferSize];
        await using var stream = _client.GetStream();
        try
        {
            while (token.IsCancellationRequested is false)
            {
                try
                {
                    await ReadNextRequestAsync(stream, requestBuffer, buffer, token);
                }
                catch (Exception e)
                    when (e.GetBaseException() is SocketException {SocketErrorCode: SocketError.Shutdown})
                {
                    _logger.Debug("Клиентский сокет закрылся. Закрываю соединение");
                    break;
                }

                if (token.IsCancellationRequested)
                {
                    break;
                }

                var response = _consensusModule.Handle(new SubmitRequest(requestBuffer.ToArray()));
                if (!response.WasLeader)
                {
                    // TODO: добавить ответ не лидер
                    break;
                }

                response.Response.WriteTo(stream);
            }
        }
        finally
        {
            _client.Close();
            _client.Dispose();
        }
    }

    private async Task ReadNextRequestAsync(NetworkStream networkStream,
                                            List<byte> requestBuffer,
                                            byte[] socketBuffer,
                                            CancellationToken token)
    {
        requestBuffer.Clear();
        var read = await networkStream.ReadAsync(socketBuffer, token);
        if (read < socketBuffer.Length)
        {
            requestBuffer.AddRange(socketBuffer.Take(read));
            return;
        }
        
        requestBuffer.AddRange(socketBuffer);
        while (read == socketBuffer.Length)
        {
            read = await networkStream.ReadAsync(socketBuffer, token);
            if (read == 0)
            {
                break;
            }
            requestBuffer.AddRange(socketBuffer.Take(read));
        }
    }
}