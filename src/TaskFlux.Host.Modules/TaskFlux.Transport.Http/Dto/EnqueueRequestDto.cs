using System.ComponentModel.DataAnnotations;

namespace TaskFlux.Transport.Http.Dto;

public class EnqueueRequestDto
{
    [Required(AllowEmptyStrings = true, ErrorMessage = "Название очереди не указано")]
    public string Queue { get; set; } = null!;

    public long Key { get; set; }

    [Required(AllowEmptyStrings = true, ErrorMessage = "Тело сообщения не указано")]
    public string Payload { get; set; } = null!;
}