using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands;

public class DefaultRequestSerializer
{
    public static readonly DefaultRequestSerializer Instance = new();
    
    public void Serialize(EnqueueRequest request, BinaryWriter writer)
    {
        writer.Write((int)request.Type);
        writer.Write(request.Key);
        writer.Write(request.Payload.Length);
        writer.Write(request.Payload);
    }

    public void Serialize(DequeueRequest request, BinaryWriter writer)
    {
        writer.Write((int)RequestType.DequeueRequest);
    }

    public void Serialize(GetCountRequest request, BinaryWriter writer)
    {
        writer.Write((int)RequestType.GetCountRequest);
    }
}