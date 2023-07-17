using JobQueue.Core;

namespace Consensus.StateMachine.JobQueue.Commands.Batch;

public class BatchCommand: IJobQueueCommand
{
    private readonly ICollection<IJobQueueCommand> _requests;

    public BatchCommand(ICollection<IJobQueueCommand> requests)
    {
        _requests = requests;
    }
    
    public IJobQueueResponse Apply(IJobQueue jobQueue)
    {
        var responses = new List<IJobQueueResponse>(_requests.Count);
        foreach (var request in _requests)
        {
            var response = request.Apply(jobQueue);
            responses.Add(response);
        }

        return new BatchResponse(responses);
    }

    public void ApplyNoResponse(IJobQueue jobQueue)
    {
        foreach (var request in _requests)
        {
            request.Apply(jobQueue);
        }
    }
}