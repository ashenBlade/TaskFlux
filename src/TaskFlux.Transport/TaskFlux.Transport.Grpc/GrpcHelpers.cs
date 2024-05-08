using System.Diagnostics;
using Taskflux;
using TaskFlux.Core.Commands.Error;
using TaskFlux.Core.Policies;

namespace TaskFlux.Transport.Grpc;

internal static class GrpcHelpers
{
    public static ErrorCode MapGrpcErrorCode(ErrorType type)
    {
        // Для удобства используем одни и те же коды ошибок
        var errorCode = (ErrorCode)type;
        Debug.Assert(Enum.IsDefined(errorCode), "Enum.IsDefined(errorCode)", "Неизвестный код ошибки для gRPC");
        return errorCode;
    }

    public static PolicyViolationResponse MapGrpcPolicyViolationResponse(
        Core.Commands.PolicyViolation.PolicyViolationResponse domainResponse)
    {
        return new PolicyViolationResponse()
        {
            Message = domainResponse.ViolatedPolicy.Accept(ViolatedPolicyVisitor.Instance)
        };
    }

    private class ViolatedPolicyVisitor : IQueuePolicyVisitor<string>
    {
        public static readonly ViolatedPolicyVisitor Instance = new();

        public string Visit(PriorityRangeQueuePolicy policy)
        {
            return "Неверный диапазон приоритета";
        }

        public string Visit(MaxQueueSizeQueuePolicy policy)
        {
            return "Превышен максимальный размер очереди";
        }

        public string Visit(MaxPayloadSizeQueuePolicy policy)
        {
            return "Превышен максимальный размер тела сообщения";
        }
    }
}