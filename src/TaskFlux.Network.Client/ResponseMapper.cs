using TaskFlux.Commands;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.CreateQueue;
using TaskFlux.Commands.DeleteQueue;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core.Policies;
using TaskFlux.Core.Queue;
using TaskFlux.Models;
using TaskFlux.Network.Packets.Responses;
using TaskFlux.Network.Packets.Responses.Policies;

namespace TaskFlux.Network.Client;

public static class ResponseMapper
{
    public static NetworkResponse Map(Response response)
    {
        return response.Accept(ResponseMapperVisitor.Instance);
    }

    private class ResponseMapperVisitor
        : IResponseVisitor<NetworkResponse>,
          IQueuePolicyVisitor<NetworkQueuePolicy>
    {
        public static readonly ResponseMapperVisitor Instance = new();

        #region Domain -> Network

        public NetworkResponse Visit(DequeueResponse response)
        {
            if (response.TryGetResult(out var key, out var data))
            {
                return new DequeueNetworkResponse(( key, data ));
            }

            return new DequeueNetworkResponse(null);
        }

        public NetworkResponse Visit(EnqueueResponse response)
        {
            return OkNetworkResponse.Instance;
        }

        public NetworkResponse Visit(CreateQueueResponse response)
        {
            return OkNetworkResponse.Instance;
        }

        public NetworkResponse Visit(DeleteQueueResponse response)
        {
            return OkNetworkResponse.Instance;
        }

        public NetworkResponse Visit(CountResponse response)
        {
            return new CountNetworkResponse(response.Count);
        }

        public NetworkResponse Visit(ErrorResponse response)
        {
            return new ErrorNetworkResponse(( byte ) response.ErrorType, response.Message);
        }

        public NetworkResponse Visit(OkResponse response)
        {
            return OkNetworkResponse.Instance;
        }

        public NetworkResponse Visit(ListQueuesResponse response)
        {
            return new ListQueuesNetworkResponse(response.Metadata.Select(m => new MetadataTaskQueueInfo(m)).ToArray());
        }

        private class MetadataTaskQueueInfo : ITaskQueueInfo
        {
            private readonly ITaskQueueMetadata _metadata;

            public MetadataTaskQueueInfo(ITaskQueueMetadata metadata)
            {
                _metadata = metadata;
            }

            public QueueName QueueName => _metadata.QueueName;
            public int Count => _metadata.Count;
            private Dictionary<string, string>? _policiesCache;
            public Dictionary<string, string> Policies => _policiesCache ??= BuildPolicyCache();

            private Dictionary<string, string> BuildPolicyCache()
            {
                var policies = new Dictionary<string, string>();
                if (_metadata.MaxQueueSize is { } maxQueueSize)
                {
                    policies["max-queue-size"] = maxQueueSize.ToString();
                }

                if (_metadata.MaxPayloadSize is { } maxPayloadSize)
                {
                    policies["max-payload-size"] = maxPayloadSize.ToString();
                }

                if (_metadata.PriorityRange is var (min, max))
                {
                    policies["priority-range"] = $"{min} {max}";
                }

                return policies;
            }
        }

        public NetworkResponse Visit(PolicyViolationResponse response)
        {
            return new PolicyViolationNetworkResponse(response.ViolatedPolicy.Accept(this));
        }

        #endregion

        #region DomainPolicy -> NetworkPolicy

        public NetworkQueuePolicy Visit(PriorityRangeQueuePolicy policy)
        {
            return new PriorityRangeNetworkQueuePolicy(policy.Min, policy.Max);
        }

        public NetworkQueuePolicy Visit(MaxQueueSizeQueuePolicy policy)
        {
            return new MaxQueueSizeNetworkQueuePolicy(policy.MaxQueueSize);
        }

        public NetworkQueuePolicy Visit(MaxPayloadSizeQueuePolicy policy)
        {
            return new MaxMessageSizeNetworkQueuePolicy(policy.MaxPayloadSize);
        }

        #endregion
    }
}