using JobQueue.Core;
using JobQueue.InMemory;
using JobQueue.PriorityQueue.StandardLibrary;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;

namespace TaskFlux.Commands.CreateQueue;

public class CreateQueueCommand : Command
{
    public override CommandType Type => CommandType.CreateQueue;
    public QueueName Name { get; }
    public uint Size { get; }
    public bool HasLimit => Size == 0;

    private IJobQueue CreateJobQueue() =>
        new PrioritizedJobQueue(Name, Size, new StandardLibraryPriorityQueue<long, byte[]>());

    public CreateQueueCommand(QueueName name, uint size)
    {
        Name = name;
        Size = size;
    }

    public override Result Apply(ICommandContext context)
    {
        var manager = context.Node.GetJobQueueManager();
        if (!manager.HasQueue(Name))
        {
            return DefaultErrors.QueueAlreadyExists;
        }

        var queue = CreateJobQueue();
        if (manager.TryAddQueue(Name, queue))
        {
            return OkResult.Instance;
        }

        return new ErrorResult(ErrorType.Unknown, "Не удалось создать указанную очередь. Неизвестная ошибка");
    }

    public override void ApplyNoResult(ICommandContext context)
    {
        var manager = context.Node.GetJobQueueManager();
        if (!manager.HasQueue(Name))
        {
            return;
        }

        var queue = CreateJobQueue();
        manager.TryAddQueue(Name, queue);
    }

    public override void Accept(ICommandVisitor visitor)
    {
        visitor.Visit(this);
    }

    public override T Accept<T>(IReturningCommandVisitor<T> visitor)
    {
        return visitor.Visit(this);
    }

    public override ValueTask AcceptAsync(IAsyncCommandVisitor visitor, CancellationToken token = default)
    {
        return visitor.VisitAsync(this, token);
    }
}