using TaskFlux.Core;
using TaskFlux.Requests;
using TaskFlux.Requests.Batch;

namespace Consensus.StateMachine.JobQueue.Serializer.Commands;

public class BatchCommand: ICommand
{
    private readonly ICollection<ICommand> _commands;

    public BatchCommand(ICollection<ICommand> commands)
    {
        _commands = commands;
    }


    public IResponse Apply(INode node)
    {
        if (_commands.Count == 0)
        {
            return BatchResponse.Empty;
        }

        var responses = new List<IResponse>(_commands.Count);
        
        foreach (var command in _commands)
        {
            var response = command.Apply(node);
            responses.Add(response);
        }

        return new BatchResponse(responses);
    }

    public void ApplyNoResponse(INode node)
    {
        if (_commands.Count == 0)
        {
            return;
        }

        foreach (var command in _commands)
        {
            command.ApplyNoResponse(node);
        }
    }
}