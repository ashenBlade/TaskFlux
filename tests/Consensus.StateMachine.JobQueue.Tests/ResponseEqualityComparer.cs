using TaskFlux.Requests;
using TaskFlux.Requests.Batch;
using TaskFlux.Requests.Dequeue;
using TaskFlux.Requests.Enqueue;
using TaskFlux.Requests.Error;
using TaskFlux.Requests.Requests.JobQueue.GetCount;

namespace Consensus.StateMachine.JobQueue.Tests;

public class ResponseEqualityComparer: IEqualityComparer<IResponse>
{
    public static readonly ResponseEqualityComparer Instance = new();
    public bool Equals(IResponse? x, IResponse? y)
    {
        return x is not null && 
               y is not null && 
               Check(( dynamic ) x, ( dynamic ) y);
    }

    private bool Check(EnqueueResponse first, EnqueueResponse second) => first.Success == second.Success;
    private bool Check(DequeueResponse first, DequeueResponse second) =>
        ( first.Success, second.Success ) switch
        {
            (true, true)   => first.Key == second.Key && first.Payload.SequenceEqual(second.Payload),
            (false, false) => true,
            _              => false
        };

    private bool Check(GetCountResponse first, GetCountResponse second) => first.Count == second.Count;
    private bool Check(ErrorResponse first, ErrorResponse second) => first.Message == second.Message;
    private bool Check(BatchResponse first, BatchResponse second)
    {
        if (first.Responses.Count != second.Responses.Count)
        {
            return false;
        }

        foreach (var (x, y) in first.Responses.Zip(second.Responses))
        {
            if (!Check((dynamic)x, (dynamic)y))
            {
                return false;
            }
        }

        return true;
    }

    private bool Check(IResponse _, IResponse __) => false;

    public int GetHashCode(IResponse obj)
    {
        return obj.GetHashCode();
    }
}