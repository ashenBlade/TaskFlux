using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands.Serializers;

public class JobQueueResponseSerializer
{
    public static readonly JobQueueResponseSerializer Instance = new();
    
    public void Serialize(IJobQueueResponse response, BinaryWriter writer)
    {
        writer.Write((int)response.Type);
        response.Accept(new SerializerJobQueueResponseVisitor(writer));
    }

    private class SerializerJobQueueResponseVisitor : IJobQueueResponseVisitor
    {
        private readonly BinaryWriter _writer;

        public SerializerJobQueueResponseVisitor(BinaryWriter writer)
        {
            _writer = writer;
        }
        
        public void Visit(DequeueResponse response)
        {
            _writer.Write(response.Success);
            if (response.Success)
            {
                _writer.Write(response.Key);
                _writer.Write(response.Payload.Length);
                _writer.Write(response.Payload);
            }
        }

        public void Visit(EnqueueResponse response)
        {
            _writer.Write(response.Success);
        }

        public void Visit(GetCountResponse response)
        {
            _writer.Write(response.Count);
        }

        public void Visit(ErrorResponse response)
        {
            _writer.Write(response.Message);
        }
    }
}