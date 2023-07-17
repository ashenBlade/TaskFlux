using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Serialization;

public class JobQueueCommandDeserializer: ICommandDeserializer
{
    private readonly IJobQueueRequestDeserializer _deserializer;
    private readonly IJobQueueResponseSerializer _serializer;

    public JobQueueCommandDeserializer(IJobQueueRequestDeserializer deserializer, IJobQueueResponseSerializer serializer)
    {
        _deserializer = deserializer;
        _serializer = serializer;
    }
    
    public ICommand Deserialize(byte[] payload)
    {
        var request = _deserializer.Deserialize(payload);
        var visitor = new CommandBuilderRequestVisitor();
        request.Accept(visitor);
        return new SerializerJobQueueCommand( visitor.GetResultCheck(), _serializer);
    }

    private class CommandBuilderRequestVisitor : IJobQueueRequestVisitor
    {
        public IJobQueueCommand? Result { get; private set; }
        public IJobQueueCommand GetResultCheck() => Result ?? throw new ApplicationException(
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
            var commands = new List<IJobQueueCommand>();
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