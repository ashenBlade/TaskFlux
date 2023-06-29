using System.Net;
using System.Net.Sockets;
using Serilog;
using Serilog.Core;

namespace Raft.Network.Socket;

public class RemoteSocketNodeConnection: SocketNodeConnection, IRemoteNodeConnection
{
    private readonly EndPoint? _endPoint;
    private readonly ILogger _logger;
    private readonly TimeSpan _connectTimeout;

    public bool Connected => Socket.Connected;

    public RemoteSocketNodeConnection(EndPoint endPoint, ILogger logger, TimeSpan connectTimeout)
        : base(new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
    {
        _endPoint = endPoint;
        _logger = logger;
        _connectTimeout = connectTimeout;
    }

    public RemoteSocketNodeConnection(System.Net.Sockets.Socket existingSocket, ILogger logger) 
        : base(existingSocket)
    {
        _logger = logger;
        _endPoint = null;
    }

    public async ValueTask DisconnectAsync(CancellationToken token = default)
    {
        if (!Socket.Connected)
        {
            return;
        }

        _logger.Verbose("Отключаю соединение");
        try
        {
            await Socket.DisconnectAsync(true, token);
            _logger.Verbose("Соединение отключено");
        }
        catch (SocketException se) when (se.SocketErrorCode is SocketError.NotConnected)
        {
            _logger.Verbose("Запрошено разъединение с неподключенным узлом");
        }
    }

    public async ValueTask<bool> ConnectAsync(CancellationToken token = default)
    {
        if (_endPoint is null)
        {
            throw new InvalidOperationException("Невозможно подключиться. Адрес для подключения не указан");
        }
        
        if (Socket.Connected)
        {
            _logger.Verbose("Отключаю соединение");
            await Socket.DisconnectAsync(true, token);
        }
        
        return await ConnectAsyncCore(token);
    }

    private async ValueTask<bool> ConnectAsyncCore(CancellationToken token)
    {
        try
        {
            // При установлении подключения возможна такая ситуация:
            // 1. Сервер (другой узел/сокет) только запускается
            // 2. Клиент (мы) начинает подключаться - ConnectAsync
            // 3. Сервер по какой-то причине не принимает сокет/запрос (просто не видит)
            // 4. Клиент не получает уведомления - ConnectAsync не возвращается

            // Этот баг повторяется от раза к разу,
            // но приводит к серьезным последствиям 
            
            // После определенного времени ConnectAsync вернется,
            // но проблема в том, что другой узел может к нам подключиться:
            // 1. Узел запускается
            // 2. Читает СВОЙ СТАРЫЙ лог
            // 3. Из-за бага выше, к нему не подключиться
            // 4. Срабатывает Election Timeout
            // 5. Узел отправляет RequestVote 
            // 6. Другие узлы отклоняют запрос и передают СВОЙ (БОЛЬШИЙ) терм
            
            // Приходим к такой ситуации:
            // - Наш узел: старый лог (который к тому же не обновляется), терм как у всех
            // - Другие узлы: работают корректно и терм актуальный
            // 1. Срабатывает Election Timeout 
            // 2. Узел переходит в новый терм (больший среди всех)
            // 3. Отправляет RequestVote на другие узлы
            // 4. ВСЕ узлы отвечают согласием, т.к. терм больше
            // 5. Узел становится лидером
            // 6. Глобальный лог откатывается к состоянию старого узла
            
            // Чтобы с этим бороться, используется таймаут.
            // !!! Создание нового CancellationTokenSource с таймаутом не работает (причина не ясна)
            // Поэтому использую Task.Delay( /*timeout*/ ) с Task.WhenAny
            
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            var connectTask = Socket.ConnectAsync(_endPoint!, cts.Token).AsTask();
            var delayTask = Task.Delay(_connectTimeout, CancellationToken.None);
            _logger.Debug("Начинаю подключение");
            await Task.WhenAny(connectTask, delayTask);
            _logger.Debug("Заканчиваю подключение");
            if (delayTask.IsCompleted)
            {
                _logger.Debug("Превышен таймаут запроса подключения");
                cts.Cancel();
                return false;
            }
            
            _logger.Debug("Подключение ОК");
            await connectTask;
            _logger.Debug("Подключение ОК 2");
            
            return true;
        }
        catch (SocketException se) when (se.SocketErrorCode is
                                             SocketError.TimedOut or 
                                             
                                             SocketError.ConnectionRefused or 
                                             SocketError.ConnectionAborted or 
                                             SocketError.ConnectionReset or
                                             
                                             SocketError.NetworkDown or
                                             SocketError.NetworkReset or
                                             SocketError.NetworkUnreachable or
                                             
                                             SocketError.HostNotFound or
                                             SocketError.HostUnreachable or
                                             SocketError.HostDown)
        { }

        return false;
    }
}