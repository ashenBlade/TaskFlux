using Consensus.StateMachine.JobQueue.Commands;
using Consensus.StateMachine.JobQueue.Commands.Batch;
using Consensus.StateMachine.JobQueue.Commands.Dequeue;
using Consensus.StateMachine.JobQueue.Commands.Enqueue;
using Consensus.StateMachine.JobQueue.Commands.Error;
using Consensus.StateMachine.JobQueue.Commands.GetCount;

namespace Consensus.StateMachine.JobQueue.Serialization;

public class JobQueueResponseSerializer : IJobQueueResponseSerializer
{
    public static readonly JobQueueResponseSerializer Instance = new();
    
    public void Serialize(IJobQueueResponse response, BinaryWriter writer)
    {
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
            _writer.Write((int)response.Type);
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
            _writer.Write((int)response.Type);
            _writer.Write(response.Success);
        }

        public void Visit(GetCountResponse response)
        {
            _writer.Write((int)response.Type);
            _writer.Write(response.Count);
        }

        public void Visit(ErrorResponse response)
        {
            _writer.Write((int)response.Type);
            _writer.Write(response.Message);
        }

        public void Visit(BatchResponse response)
        {
            _writer.Write((int)response.Type);
            _writer.Write(response.Responses.Count);
            foreach (var innerResponse in response.Responses)
            {
                innerResponse.Accept(this);
            }
        }
    }
}