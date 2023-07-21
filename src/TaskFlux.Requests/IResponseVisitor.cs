using TaskFlux.Requests.GetCount;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Error;
using TaskFlux.Requests.Requests.JobQueue.GetCount;

namespace TaskFlux.Requests;

public interface IResponseVisitor
{
    public void Visit(GetCountResponse response);
    public void Visit(EnqueueResponse response);
    public void Visit(DequeueResponse response);
    
    public void Visit(ErrorResponse response);
    public void Visit(BatchResponse batchResponse);
}