namespace Consensus.Core.Log;

public interface ISnapshot
{
    /// <summary>
    /// Записать данные снапшота в переданный поток
    /// </summary>
    /// <param name="stream">Поток (файл), куда нужно записывать данные снапшота</param>
    /// <param name="token">Токен отмены</param>
    public void WriteTo(Stream stream, CancellationToken token = default);
}