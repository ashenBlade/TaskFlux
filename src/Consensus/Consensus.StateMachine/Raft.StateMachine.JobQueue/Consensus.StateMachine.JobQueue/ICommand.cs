using TaskFlux.Requests;
using TaskFlux.Core;

namespace Consensus.StateMachine.JobQueue;

public interface ICommand
{
    public IResponse Apply(INode node);
    public void ApplyNoResponse(INode node);
}