namespace Consensus.Raft;

public interface ITimerFactory
{
    /// <summary>
    /// Метод для создания нового объекта таймера.
    /// </summary>
    /// <returns>Новый таймер</returns>
    /// <remarks>Для создаваемых объектов нужно вызывать <see cref="IDisposable.Dispose"/> самим</remarks>
    public ITimer CreateTimer();
}