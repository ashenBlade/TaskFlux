using System.Net.Sockets;
using Raft.Peer.Exceptions;

namespace Raft.Peer;

public interface ISocket: IDisposable
{
    /// <summary>
    /// Отправить переданный пакет байтов по сети по сокету
    /// </summary>
    /// <param name="payload">Данные для пересылки</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="TaskCanceledException">Операция была отменена</exception>
    /// <exception cref="NetworkException">Во время отправки произошла ошибка сети</exception>
    /// <exception cref="SocketException">Неизвестная ошибка произошла во время отправки</exception>
    public Task SendAsync(ReadOnlyMemory<byte> payload, CancellationToken token = default);
    
    /// <summary>
    /// Получить передаваемый по сети пакет и записать полученные данные в переданный <paramref name="stream"/>
    /// </summary>
    /// <param name="stream">Поток для записи получаемых данных</param>
    /// <param name="token">Токен отмены</param>
    /// <exception cref="TaskCanceledException">Операция была отменена</exception>
    /// <exception cref="NetworkException">Во время отправки произошла ошибка сети</exception>
    /// <exception cref="SocketException">Неизвестная ошибка произошла во время отправки</exception>
    /// <exception cref="NotSupportedException">Переданный <paramref name="stream"/> не поддерживает запись</exception>
    public ValueTask ReadAsync(Stream stream, CancellationToken token = default);
}