using TaskFlux.Core.Commands.Count;
using TaskFlux.Core.Commands.CreateQueue;
using TaskFlux.Core.Commands.DeleteQueue;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Commands.ListQueues;
using TaskFlux.Core.Commands.Ok;
using TaskFlux.Core.Commands.PolicyViolation;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Persistence.ApplicationState.Deltas;

namespace TaskFlux.Persistence.ApplicationState;

public class DeltaExtractorResponseVisitor : IResponseVisitor<Delta?>
{
    private static Delta? NoDelta => null;

    public Delta? Visit(DequeueResponse response)
    {
        if (response.Persistent && response.TryGetResult(out var queueName, out var key, out var payload))
        {
            return new RemoveRecordDelta(queueName, key, payload);
        }

        return NoDelta;
    }

    public Delta? Visit(EnqueueResponse response)
    {
        return new AddRecordDelta(response.QueueName, response.Key, response.Message);
    }

    public Delta? Visit(CreateQueueResponse response)
    {
        return new CreateQueueDelta(response.QueueName, response.Code, response.MaxQueueSize, response.MaxMessageSize,
            response.PriorityRange);
    }

    public Delta? Visit(DeleteQueueResponse response)
    {
        return new DeleteQueueDelta(response.QueueName);
    }

    public Delta? Visit(CountResponse response)
    {
        return NoDelta;
    }

    public Delta? Visit(ErrorResponse response)
    {
        return NoDelta;
    }

    public Delta? Visit(OkResponse response)
    {
        return NoDelta;
    }

    public Delta? Visit(ListQueuesResponse response)
    {
        return NoDelta;
    }

    public Delta? Visit(PolicyViolationResponse response)
    {
        return NoDelta;
    }
}