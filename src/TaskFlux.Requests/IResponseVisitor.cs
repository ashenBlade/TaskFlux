using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Commands.JobQueue.GetCount;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Error;

namespace TaskFlux.Requests;

public interface IResponseVisitor
{
    public void Visit(GetCountResponse getCountResponse);
    public void Visit(EnqueueResponse enqueueResponse);
    public void Visit(DequeueResponse dequeueResponse);
    
    public void Visit(ErrorResponse errorResponse);
    public void Visit(BatchResponse batchResponse);
}