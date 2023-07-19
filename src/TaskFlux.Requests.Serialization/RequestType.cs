namespace TaskFlux.Requests.Serialization;

public enum RequestType
{
    EnqueueRequest,
    DequeueRequest,
    GetCountRequest,
    BatchRequest,
}