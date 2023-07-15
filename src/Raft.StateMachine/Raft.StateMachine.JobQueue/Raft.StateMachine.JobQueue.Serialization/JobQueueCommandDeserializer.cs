using Raft.StateMachine.JobQueue.Commands;
using Raft.StateMachine.JobQueue.Commands.Batch;
using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Serialization;

public class JobQueueCommandDeserializer: ICommandDeserializer
{
    private readonly IJobQueueRequestDeserializer _deserializer;

    public JobQueueCommandDeserializer(IJobQueueRequestDeserializer deserializer)
    {
        _deserializer = deserializer;
    }
    
    public ICommand Deserialize(byte[] payload)
    {
        var request = _deserializer.Deserialize(payload);
        var visitor = new CommandBuilderRequestVisitor();
        request.Accept(visitor);
        return visitor.GetResultCheck();
    }

    private class CommandBuilderRequestVisitor : IJobQueueRequestVisitor
    {
        public ICommand? Result { get; private set; }
        public ICommand GetResultCheck() => Result ?? throw new ApplicationException(
            "После парсинга в Result оказался null");
        public void Visit(DequeueRequest request)
        {
            Result = new DequeueCommand(request);
        }

        public void Visit(EnqueueRequest request)
        {
            Result = new EnqueueCommand(request);
        }

        public void Visit(GetCountRequest request)
        {
            Result = new GetCountCommand(request);
        }

        public void Visit(BatchRequest request)
        {
            var commands = new List<ICommand>();
            var cachedVisitor = new CommandBuilderRequestVisitor();
            foreach (var innerRequest in request.Requests)
            {
                innerRequest.Accept(cachedVisitor);
                commands.Add(cachedVisitor.GetResultCheck());
                cachedVisitor.Result = null;
            }

            Result = new BatchCommand(commands);
        }
    }
}