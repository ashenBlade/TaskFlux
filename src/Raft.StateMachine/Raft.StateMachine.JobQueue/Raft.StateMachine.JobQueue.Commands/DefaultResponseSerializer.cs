using Raft.StateMachine.JobQueue.Commands.Dequeue;
using Raft.StateMachine.JobQueue.Commands.Enqueue;
using Raft.StateMachine.JobQueue.Commands.Error;
using Raft.StateMachine.JobQueue.Commands.GetCount;

namespace Raft.StateMachine.JobQueue.Commands;

public class DefaultResponseSerializer
{
    public static readonly DefaultResponseSerializer Instance = new();
    
    public void Serialize(IDefaultResponse response, BinaryWriter writer)
    {
        writer.Write((int)response.Type);
        switch (response.Type)
        {
            case ResponseType.Dequeue:
                Serialize((DequeueResponse)response, writer);
                break;
            case ResponseType.Enqueue:
                Serialize((EnqueueResponse)response, writer);
                break;
            case ResponseType.GetCount:
                Serialize((GetCountResponse)response, writer);
                break;
            case ResponseType.Error:
                Serialize((ErrorResponse)response, writer);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(response.Type), response.Type, "Неизвестный тип ResponseType");
        }
        
    }

    private void Serialize(DequeueResponse response, BinaryWriter writer)
    {
        writer.Write(response.Success);
        if (response.Success)
        {
            writer.Write(response.Key);
            writer.Write(response.Payload.Length);
            writer.Write(response.Payload);
        }
    }
    
    private void Serialize(EnqueueResponse response, BinaryWriter writer)
    {
        writer.Write(response.Success);
    }

    private void Serialize(GetCountResponse response, BinaryWriter writer)
    {
        writer.Write(response.Count);
    }

    private void Serialize(ErrorResponse response, BinaryWriter writer)
    {
        writer.Write(response.Message);
    }
}