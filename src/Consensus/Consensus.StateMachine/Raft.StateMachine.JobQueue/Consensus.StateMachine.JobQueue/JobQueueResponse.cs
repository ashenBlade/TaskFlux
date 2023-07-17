using System.Text;

namespace Consensus.StateMachine.JobQueue;

public class JobQueueResponse: IResponse
{
    public ICommandResponse Response { get; }

    public JobQueueResponse(ICommandResponse response)
    {
        Response = response;
    }

    public void WriteTo(Stream output)
    {
        using var writer = new BinaryWriter(output, Encoding.UTF8, leaveOpen: true);
        Response.WriteTo(writer);
        writer.Flush();
    }
}