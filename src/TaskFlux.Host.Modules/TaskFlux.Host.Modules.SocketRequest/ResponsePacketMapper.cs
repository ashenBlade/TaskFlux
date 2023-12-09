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
using TaskFlux.Network;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Responses;
using TaskFlux.Network.Responses.Policies;

namespace TaskFlux.Host.Modules.SocketRequest;

public static class ResponsePacketMapper
{
    public static Packet MapResponse(Response response)
    {
        return response.Accept(ResponseMapperVisitor.Instance);
    }

    private class ResponseMapperVisitor
        : IResponseVisitor<Packet>,
          IQueuePolicyVisitor<NetworkQueuePolicy>
    {
        public static readonly ResponseMapperVisitor Instance = new();

        public Packet Visit(DequeueResponse response)
        {
            if (response.TryGetResult(out var key, out var data))
            {
                return new CommandResponsePacket(new DequeueNetworkResponse(( key, data )));
            }

            return new CommandResponsePacket(new DequeueNetworkResponse(null));
        }

        public Packet Visit(EnqueueResponse response)
        {
            return OkPacket.Instance;
        }

        public Packet Visit(CreateQueueResponse response)
        {
            return OkPacket.Instance;
        }

        public Packet Visit(DeleteQueueResponse response)
        {
            return OkPacket.Instance;
        }

        public Packet Visit(CountResponse response)
        {
            return new CommandResponsePacket(new CountNetworkResponse(response.Count));
        }

        public Packet Visit(ErrorResponse response)
        {
            return new CommandResponsePacket(new ErrorNetworkResponse(( byte ) response.ErrorType, response.Message));
        }

        public Packet Visit(OkResponse response)
        {
            return OkPacket.Instance;
        }

        public Packet Visit(ListQueuesResponse response)
        {
            return new CommandResponsePacket(new ListQueuesNetworkResponse(
                response.Metadata.Select(m => new MetadataTaskQueueInfo(m)).ToArray()));
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

        public Packet Visit(PolicyViolationResponse response)
        {
            return new CommandResponsePacket(new PolicyViolationNetworkResponse(response.ViolatedPolicy.Accept(this)));
        }

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
    }
}