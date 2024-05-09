using TaskFlux.Network;
using TaskFlux.Network.Packets;
using TaskFlux.Network.Responses;
using TaskFlux.Transport.Tcp.Client.Exceptions;

namespace TaskFlux.Transport.Tcp.Client;

internal static class Helpers
{
    public static void CheckNotErrorResponse(Packet packet)
    {
        if (packet.Type == PacketType.CommandResponse)
        {
            var commandResponse = (CommandResponsePacket)packet;
            var networkResponse = commandResponse.Response;
            if (networkResponse.Type == NetworkResponseType.Error)
            {
                var errorResponse = (ErrorNetworkResponse)networkResponse;
                throw new ErrorResponseException(errorResponse.ErrorType, errorResponse.Message);
            }

            if (networkResponse.Type == NetworkResponseType.PolicyViolation)
            {
                var policyViolation = (PolicyViolationNetworkResponse)networkResponse;
                throw new PolicyViolationException(policyViolation.ViolatedNetworkQueuePolicy);
            }
        }

        if (packet.Type == PacketType.ErrorResponse)
        {
            var errorResponse = (ErrorResponsePacket)packet;
            throw new ErrorResponseException(errorResponse.ErrorType, errorResponse.Message);
        }

        if (packet.Type == PacketType.NotLeader)
        {
            var leaderPacket = (NotLeaderPacket)packet;
            throw new NotLeaderException(leaderPacket.LeaderId);
        }
    }
}