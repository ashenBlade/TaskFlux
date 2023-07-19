using JobQueue.Core;
using TaskFlux.Requests;
using TaskFlux.Requests.Commands.JobQueue.GetCount;
using TaskFlux.Requests.GetCount;

namespace Consensus.StateMachine.JobQueue.Serializer.Commands;

public class GetCountCommand: JobQueueCommand
{
    // На будущее
    // ReSharper disable once NotAccessedField.Local
    private readonly GetCountRequest _request;

    public GetCountCommand(GetCountRequest request)
    {
        _request = request;
    }
    
    protected override IResponse Apply(IJobQueue jobQueue)
    {
        var count = jobQueue.Count;
        if (count == 0)
        {
            return GetCountResponse.Empty;
        }

        return new GetCountResponse(count);
    }

    protected override void ApplyNoResponse(IJobQueue jobQueue)
    { }
}