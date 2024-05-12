namespace TaskFlux.Network.Responses;

public enum NetworkResponseType : byte
{
    Count = (byte)'c',
    Error = (byte)'e',

    Dequeue = (byte)'d',
    ListQueues = (byte)'l',
    PolicyViolation = (byte)'p',
}