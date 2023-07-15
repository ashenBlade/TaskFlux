using System.Text;
using Raft.StateMachine.JobQueue.Commands;

namespace Raft.StateMachine.JobQueue;

public class JobQueueResponse: IResponse
{
    public IJobQueueResponse Response { get; }

    public JobQueueResponse(IJobQueueResponse response)
    {
        Response = response;
    }

    public void WriteTo(Stream output)
    {
        using var writer = new BinaryWriter(output, Encoding.UTF8, leaveOpen: true);
        
        writer.Flush();
    }
}