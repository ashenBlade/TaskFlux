using Consensus.Core.Persistence;

namespace Consensus.Persistence.Snapshot;

/// <summary>
/// Объект снапшота читающий данные с потока (файла).
/// После выполнения операции поток закрывается
/// </summary>
public class FileSnapshot : ISnapshot
{
    private readonly Stream _stream;

    /// <summary>
    /// Основной конструктор объекта снапшота
    /// </summary>
    /// <param name="stream">Поток файла</param>
    /// <remarks>
    /// Позиция потока должна быть выставлена на начало блока данных снапшота
    /// </remarks>
    public FileSnapshot(Stream stream)
    {
        _stream = stream;
    }

    public void WriteTo(Stream stream, CancellationToken token = default)
    {
        _stream.CopyTo(stream);
        _stream.Close();
    }
}