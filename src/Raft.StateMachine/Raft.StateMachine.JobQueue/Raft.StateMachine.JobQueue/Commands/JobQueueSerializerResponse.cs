using Raft.StateMachine.JobQueue.Commands.Serializers;

namespace Raft.StateMachine.JobQueue.Commands;

public class JobQueueSerializerResponse: IJobQueueCommandResponse
{
    public IJobQueueResponse Response { get; }
    private readonly IJobQueueResponseSerializer _serializer;

    public JobQueueSerializerResponse(IJobQueueResponse response, IJobQueueResponseSerializer serializer)
    {
        Response = response;
        _serializer = serializer;
    }
    public void WriteTo(BinaryWriter writer)
    {
        _serializer.Serialize(Response, writer); 
    }
}