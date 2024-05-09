using TaskFlux.Core;
using TaskFlux.Network.Responses;
using TaskFlux.Transport.Tcp.Client.Exceptions;

namespace TaskFlux.Transport.Tcp.Client;

/// <summary>
/// Декоратор, который переподключается к узлам кластера при различных обстоятельствах:
/// - Разрыв соединения
/// - Выставление нового лидера кластера
/// </summary>
internal class ReconnectingTaskFluxClientDecorator : ITaskFluxClient
{
    private TaskFluxClient _client;
    private readonly TaskFluxClientFactory _factory;

    internal ReconnectingTaskFluxClientDecorator(TaskFluxClient client, TaskFluxClientFactory factory)
    {
        _client = client;
        _factory = factory;
    }

    public async Task EnqueueAsync(QueueName queue, long key, byte[] message, CancellationToken token = default)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                await _client.EnqueueAsync(queue, key, message, token);
                return;
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        token.ThrowIfCancellationRequested();
    }

    public async Task<(long Key, byte[] Message)> DequeueAsync(QueueName queue, CancellationToken token = default)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                return await _client.DequeueAsync(queue, token);
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        throw new OperationCanceledException(token);
    }

    public async Task<List<ITaskQueueInfo>> GetAllQueuesInfoAsync(CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                return await _client.GetAllQueuesInfoAsync(token);
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        throw new OperationCanceledException(token);
    }

    public async Task<int> GetQueueLengthAsync(QueueName queueName, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                return await _client.GetQueueLengthAsync(queueName, token);
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        throw new OperationCanceledException(token);
    }

    public async Task CreateQueueAsync(QueueName queueName, CreateQueueOptions options, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                await _client.CreateQueueAsync(queueName, options, token);
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        throw new OperationCanceledException(token);
    }

    public async Task DeleteQueueAsync(QueueName queueName, CancellationToken token)
    {
        while (token.IsCancellationRequested is false)
        {
            try
            {
                await _client.DeleteQueueAsync(queueName, token);
            }
            catch (NotLeaderException nle)
            {
                _factory.LeaderId = nle.CurrentLeaderId;
            }
            catch (EndOfStreamException)
            {
            }
            catch (IOException)
            {
            }

            await _client.DisposeAsync();
            _client = await _factory.ConnectCoreAsync(token);
        }

        throw new OperationCanceledException(token);
    }

    public void Dispose()
    {
        _client.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
    }
}