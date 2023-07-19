using Consensus.StateMachine.JobQueue.Serializer.Commands;
using TaskFlux.Requests;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Requests.JobQueue.Enqueue;

namespace Consensus.StateMachine.JobQueue.Serializer;

public class DefaultCommandBuilder: ICommandBuilder
{
    public static readonly DefaultCommandBuilder Instance = new();
    public ICommand BuildCommand(IRequest request)
    {
        var visitor = new CommandBuilderRequestVisitor();
        request.Accept(visitor);
        return visitor.Command 
            ?? throw new ApplicationException($"Посетитель команд вернул null в свойстве {nameof(CommandBuilderRequestVisitor.Command)}");
    }

    private class CommandBuilderRequestVisitor : IRequestVisitor
    {
        public ICommand? Command { get; private set; }
        
        public void Visit(GetCountRequest getCountRequest)
        {
            Command = new GetCountCommand(getCountRequest);
        }

        public void Visit(EnqueueRequest enqueueRequest)
        {
            Command = new EnqueueCommand(enqueueRequest);
        }

        public void Visit(DequeueRequest dequeueRequest)
        {
            Command = new DequeueCommand(dequeueRequest);
        }

        public void Visit(BatchRequest batchRequest)
        {
            if (batchRequest.Requests.Count == 0)
            {
                Command = new BatchCommand(Array.Empty<ICommand>());
                return;
            }
            
            var innerVisitor = new CommandBuilderRequestVisitor();
            var commands = new List<ICommand>(batchRequest.Requests.Count);
            foreach (var request in batchRequest.Requests)
            {
                request.Accept(innerVisitor);
                commands.Add(innerVisitor.Command ?? throw new ApplicationException($"Посетитель команд вернул null в свойстве {nameof(Command)}") );    
            }
            
            Command = new BatchCommand(commands);
        }
    }
}