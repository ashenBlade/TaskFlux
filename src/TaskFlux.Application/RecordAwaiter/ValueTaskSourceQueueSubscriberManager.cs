using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.ObjectPool;
using TaskFlux.Core.Subscription;
using TaskFlux.Domain;

namespace TaskFlux.Application.RecordAwaiter;

public class ValueTaskSourceQueueSubscriberManager : IQueueSubscriberManager
{
    /// <summary>
    /// Общий пул ожидателей, которые используются для создания новых (получения)
    /// </summary>
    private readonly ObjectPool<QueueSubscriber> _awaiterPool;

    /// <summary>
    /// Ждуны, которые в данный момент находятся в работе и ожидают новых записей.
    /// </summary>
    /// <remarks>
    /// Несмотря на то, что выполнение операций однопоточное (очередь запросов), работа менеджера - асинхронная, т.к. клиент может выполнить операцию в любой момент.
    /// </remarks>
    private readonly ConcurrentQueue<QueueSubscriber> _pending;

    public ValueTaskSourceQueueSubscriberManager(ObjectPool<QueueSubscriber> awaiterPool)
    {
        _awaiterPool = awaiterPool;
        _pending = new();
    }

    public bool TryNotifyRecord(QueueRecord record)
    {
        while (_pending.TryDequeue(out var waiter))
        {
            if (waiter.TryNotifyRecord(this, record))
            {
                return true;
            }

            // Если уведомить не смогли, то обратно его добавлять не будем - скорее всего 
        }

        return false;
    }

    public IQueueSubscriber GetSubscriber()
    {
        var waiter = _awaiterPool.Get();

        // Вначале выставляем себя родителем и только после помещаем в очередь ожиданий
        waiter.Prepare(this);
        _pending.Enqueue(waiter);
        return waiter;
    }

    public class QueueSubscriber : IQueueSubscriber, IValueTaskSource<QueueRecord>
    {
        /// <summary>
        /// Регистрация отмены для переданного токена
        /// </summary>
        private CancellationTokenRegistration _tokenRegistration;

        /// <summary>
        /// Токен отмены, который мы запомнили с момента последнего запуска
        /// </summary>
        private CancellationToken _token;

        /// <summary>
        /// Реализация логики <see cref="IValueTaskSource{TResult}"/>
        /// </summary>
        private ManualResetValueTaskSourceCore<QueueRecord> _source;

        /// <summary>
        /// Менеджер ждунов, которому мы принадлежим.
        /// Может быть <c>null</c> если не работаем/закончили работать
        /// </summary>
        /// <remarks>
        /// Это volatile поле - создает барьер, чтобы избежать гонки между занулением и возвращением обратно в пул
        /// </remarks>
        private volatile ValueTaskSourceQueueSubscriberManager? _parent;

        /// <summary>
        /// Метод для подготовки объекта к работе - выставляем родителя этого <see cref="QueueSubscriber"/>
        /// </summary>
        /// <param name="parent">Объект, которому принадлежит подписчик</param>
        public void Prepare(ValueTaskSourceQueueSubscriberManager parent)
        {
            _parent = parent;
        }

        public ValueTask<QueueRecord> WaitRecordAsync(CancellationToken token = default)
        {
            Debug.Assert(_parent is not null, "_parent is not null",
                "Родитель должен быть выставлен к моменту вызова WaitRecord");

            _token = token;
            _tokenRegistration = token.Register(static o =>
            {
                var recordAwaiter = (QueueSubscriber)o!;
                Debug.Assert(recordAwaiter is not null, "recordAwaiter is not null");
                recordAwaiter._source.SetException(new OperationCanceledException(recordAwaiter._token));
            }, this);

            return new ValueTask<QueueRecord>(this, _source.Version);
        }

        public bool TryNotifyRecord(ValueTaskSourceQueueSubscriberManager parent, QueueRecord record)
        {
            // На всякий случай, проверим, что этот подписчик принадлежит нам
            if (!ReferenceEquals(parent, _parent))
            {
                return false;
            }

            try
            {
                // Перед обновлением на всякий случай проверим состояние, чтобы лишний раз не кидать исключения
                if (_source.GetStatus(_source.Version) is ValueTaskSourceStatus.Pending)
                {
                    _source.SetResult(record);
                    return true;
                }
            }
            catch (InvalidOperationException)
            {
                // На всякий случай, обработаем подобное исключение - возможно имеется гонка
            }

            return false;
        }

        /// <summary>
        /// Метод для очищения состояния и возврата объекта обратно в пул
        /// </summary>
        private void Reset()
        {
            // Очищаем токен и регистрацию отмены
            _token = default;
            _tokenRegistration.Dispose();
            _tokenRegistration = default;

            // Обновляем сам источник токенов
            _source.Reset();

            // В конце - возвращаем себя обратно в пул и очищаем состояние родителя
            Debug.Assert(_parent is not null, "_parent is not null");
            var parent = _parent;
            _parent = null; // volatile - барьер
            parent._awaiterPool.Return(this);
        }

        public void Dispose()
        {
            Reset();
        }

        #region Реализация IValueTaskSource

        public QueueRecord GetResult(short token)
        {
            return _source.GetResult(token);
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _source.GetStatus(token);
        }

        public void OnCompleted(Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            _source.OnCompleted(continuation, state, token, flags);
        }

        #endregion
    }
}