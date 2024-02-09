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
    public Delta? Visit(DequeueResponse response)
    {
        if (response.TryGetResult(out var queue, out var key, out var payload))
        {
            return new RemoveRecordDelta(queue, key, payload);
        }

        return null;
    }

    public Delta Visit(EnqueueResponse response)
    {
        return new AddRecordDelta(response.QueueName, response.Key, response.Message);
    }

    public Delta Visit(CreateQueueResponse response)
    {
        return new CreateQueueDelta(response.QueueName, response.Code, response.MaxQueueSize, response.MaxMessageSize,
            response.PriorityRange);
    }

    public Delta Visit(DeleteQueueResponse response)
    {
        return new DeleteQueueDelta(response.QueueName);
    }

    public Delta? Visit(CountResponse response)
    {
        return null;
    }

    public Delta? Visit(ErrorResponse response)
    {
        return null;
    }

    public Delta? Visit(OkResponse response)
    {
        return null;
    }

    public Delta? Visit(ListQueuesResponse response)
    {
        return null;
    }

    public Delta? Visit(PolicyViolationResponse response)
    {
        return null;
    }
}