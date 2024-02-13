using System.ComponentModel.DataAnnotations;

namespace TaskFlux.Transport.Http.Dto;

public class DequeueRequestDto
{
    [Required(AllowEmptyStrings = true, ErrorMessage = "Название очереди не указано")]
    public string Queue { get; set; } = string.Empty;
}