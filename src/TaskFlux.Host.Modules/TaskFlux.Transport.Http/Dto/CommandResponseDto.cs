namespace TaskFlux.Transport.Http.Dto;

public class CommandResponseDto
{
    public bool Success { get; set; }
    public string? Error { get; set; }
    public int? LeaderId { get; set; }
    public Dictionary<string, object?>? Payload { get; set; }
}