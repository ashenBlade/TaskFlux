using TaskFlux.Core.Commands.Count;
using TaskFlux.Core.Commands.CreateQueue;
using TaskFlux.Core.Commands.DeleteQueue;
using TaskFlux.Core.Commands.Dequeue;
using TaskFlux.Core.Commands.Enqueue;
using TaskFlux.Core.Commands.ListQueues;
using TaskFlux.Core.Commands.Visitors;
using TaskFlux.Persistence.ApplicationState.Deltas;

namespace TaskFlux.Persistence.ApplicationState;

public class DeltaExtractorCommandVisitor : ICommandVisitor<Delta?>
{
    private static Delta? NoDelta => null;

    public Delta? Visit(EnqueueCommand command)
    {
        return new AddRecordDelta(command.Queue, command.Key, command.Message);
    }

    public Delta? Visit(DequeueRecordCommand recordCommand)
    {
        return NoDelta;
    }

    public Delta? Visit(CountCommand command)
    {
        return NoDelta;
    }

    public Delta? Visit(CreateQueueCommand command)
    {
        int? maxQueueSize = command.Details.TryGetMaxQueueSize(out var mqs)
                                ? mqs
                                : null;
        int? maxMessageSize = command.Details.TryGetMaxPayloadSize(out var mms)
                                  ? mms
                                  : null;

        (long, long)? priorityRange = command.Details.TryGetPriorityRange(out var min, out var max)
                                          ? ( min, max )
                                          : null;
        return new CreateQueueDelta(command.Queue, command.Details.Code, maxQueueSize, maxMessageSize, priorityRange);
    }

    public Delta? Visit(DeleteQueueCommand command)
    {
        return new DeleteQueueDelta(command.Queue);
    }

    public Delta? Visit(ListQueuesCommand command)
    {
        return NoDelta;
    }

    public Delta? Visit(ReturnRecordCommand command)
    {
        return NoDelta;
    }

    public Delta? Visit(CommitDequeueCommand command)
    {
        if (command.Response.TryGetResult(out var queueName, out var key, out var payload))
        {
            return new RemoveRecordDelta(queueName, key, payload);
        }

        return NoDelta;
    }
}