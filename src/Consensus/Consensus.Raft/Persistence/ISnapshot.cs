namespace Consensus.Raft.Persistence;

public interface ISnapshot
{
    /// <summary>
    /// Записать данные снапшота в переданный поток
    /// </summary>
    /// <remarks>
    /// Этот метод необходимо вызывать только 1 раз,
    /// так как реализация может высвободить ресурсы после вызова (закрыть файл или сокет)
    /// </remarks>
    /// <param name="destination">Поток (файл), куда нужно записывать данные снапшота</param>
    /// <param name="token">Токен отмены</param>
    public void WriteTo(Stream destination, CancellationToken token = default);
}