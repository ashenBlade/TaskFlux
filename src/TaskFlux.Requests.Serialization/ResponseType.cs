namespace TaskFlux.Requests.Serialization;

public enum ResponseType
{
    EnqueueResponse,
    DequeueResponse,
    GetCountResponse,
    BatchResponse,
    ErrorResponse,
}