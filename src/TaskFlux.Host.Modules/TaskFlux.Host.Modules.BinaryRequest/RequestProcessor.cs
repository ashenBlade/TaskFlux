using System.Buffers;
using System.Net.Sockets;
using Consensus.Core;
using Consensus.Core.Commands.Submit;
using Serilog;
using TaskFlux.Requests;

namespace TaskFlux.Host.Modules.BinaryRequest;

internal class RequestProcessor
{
    private const int DefaultBufferSize = 256;
    private readonly TcpClient _client;
    private readonly IConsensusModule<IRequest, IResponse> _consensusModule;
    private readonly ISerializer<IRequest> _requestSerializer;
    private readonly ISerializer<IResponse> _responseSerializer;
    private readonly ILogger _logger;

    public RequestProcessor(TcpClient client, 
                            IConsensusModule<IRequest, IResponse> consensusModule,
                            ISerializer<IRequest> requestSerializer,
                            ISerializer<IResponse> responseSerializer,
                            ILogger logger)
    {
        _client = client;
        _consensusModule = consensusModule;
        _requestSerializer = requestSerializer;
        _responseSerializer = responseSerializer;
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

                var response = _consensusModule.Handle(new SubmitRequest<IRequest>(_requestSerializer.Deserialize(requestBuffer.ToArray())));
                if (!response.WasLeader)
                {
                    // TODO: добавить ответ не лидер
                    break;
                }

                await stream.WriteAsync(_responseSerializer.Serialize(response.Response), token);
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