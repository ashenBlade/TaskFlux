using System.Text;
using JobQueue.Core;

namespace Raft.StateMachine.JobQueue.Commands;

public class SerializerJobQueueCommandResponse: ICommandResponse
{
    private readonly IJobQueueResponse _response;
    private readonly IJobQueueResponseSerializer _serializer;

    public SerializerJobQueueCommandResponse(IJobQueueResponse response, IJobQueueResponseSerializer serializer)
    {
        _response = response;
        _serializer = serializer;
    }

    public void WriteTo(BinaryWriter writer)
    {
        _serializer.Serialize(_response, writer);
    }
}