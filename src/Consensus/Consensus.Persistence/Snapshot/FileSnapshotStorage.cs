using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using Consensus.Core;
using Consensus.Core.Log;
using Consensus.Persistence;

namespace Consensus.Storage.File.Snapshot;

/// <summary>
/// Файловый интерфейс взаимодействия с файлами снапшотов
/// </summary>
public class FileSnapshotStorage : ISnapshotStorage, IDisposable
{
    /// <inheritdoc cref="Constants.Marker"/>
    public const int Marker = Constants.Marker;

    /// <summary>
    /// Поток файла снапшота.
    /// <c>null</c> - файл не создан
    /// </summary>
    private Stream? _snapshotFileStream;

    /// <summary>
    /// Фабрика для создания временных файлов снапшотов
    /// </summary>
    private ITemporarySnapshotFileFactory _fileFactory;

    public FileSnapshotStorage(Stream snapshotFileStream, ITemporarySnapshotFileFactory fileFactory)
    {
        ArgumentNullException.ThrowIfNull(snapshotFileStream);
        ArgumentNullException.ThrowIfNull(fileFactory);
        _snapshotFileStream = snapshotFileStream;
        _fileFactory = fileFactory;
    }

    public FileSnapshotStorage(ITemporarySnapshotFileFactory fileFactory)
    {
        ArgumentNullException.ThrowIfNull(fileFactory);
        _fileFactory = fileFactory;
    }

    public ISnapshotFileWriter CreateTempSnapshotFile()
    {
        var tempFileStream = _fileFactory.CreateTempSnapshotFile();
        return new SnapshotFileWriter(tempFileStream, this);
    }

    public LogEntryInfo? LastLogEntry => _snapshotFileStream is null
                                             ? null
                                             : ReadLogEntryInfo();

    private LogEntryInfo ReadLogEntryInfo()
    {
        Debug.Assert(_snapshotFileStream is not null,
            "Поток файла снапшота не был инициализирован, но был вызван метод чтения заголовка");

        // Выставляем позицию сразу после маркера
        _snapshotFileStream.Position = sizeof(int);
        var reader = new BinaryReader(_snapshotFileStream, Encoding.UTF8, true);


        var index = reader.ReadInt32();
        var term = reader.ReadInt32();

        return new LogEntryInfo(new Term(term), index);
    }


    public void Dispose()
    {
        _snapshotFileStream?.Dispose();
    }

    private class SnapshotFileWriter : ISnapshotFileWriter
    {
        /// <summary>
        /// Объект, представляющий новый файл снапшота
        /// </summary>
        private ITemporarySnapshotFile _tempSnapshotFile;

        /// <summary>
        /// Поток нового файла снапшота.
        /// Инициализируется во время вызова <see cref="Initialize"/>
        /// </summary>
        private Stream _snapshotFileStream = null!;

        /// <summary>
        /// Родительский объект хранилища снапшота.
        /// Для него это все и замутили - ему нужно обновить файл снапшота
        /// </summary>
        private FileSnapshotStorage _parent;

        private enum SnapshotFileState
        {
            /// <summary>
            /// Только начинается работа
            /// </summary>
            Start = 0,

            /// <summary>
            /// Записан заголовок
            /// </summary>
            Initialized = 1,

            /// <summary>
            /// Работа с файлом закончена (вызваны <see cref="SnapshotFileWriter.Save"/> или <see cref="SnapshotFileWriter.Discard"/>
            /// </summary>
            Finished = 2,
        }

        /// <summary>
        /// Состояние файла снапшота во время его создания (записи в него)
        /// </summary>
        private SnapshotFileState _state = SnapshotFileState.Start;

        public SnapshotFileWriter(ITemporarySnapshotFile tempSnapshotTempSnapshotFile,
                                  FileSnapshotStorage parent)
        {
            _tempSnapshotFile = tempSnapshotTempSnapshotFile;
            _parent = parent;
        }

        public void Initialize(LogEntryInfo lastApplied)
        {
            switch (_state)
            {
                case SnapshotFileState.Initialized:
                    throw new InvalidOperationException(
                        "Повторная попытка записать заголовок файла снапшота: файл уже инициализирован");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Повторная попытка записать заголовок файла снапшота: работа с файлом уже завершена");
                case SnapshotFileState.Start:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            // 1. Создаем новый файл снапшота
            var stream = _tempSnapshotFile.Open();
            var writer = new BinaryWriter(stream);

            // 2. Записываем маркер файла
            writer.Write(Marker);

            // 3. Записываем заголовок основной информации
            writer.Write(lastApplied.Index);
            writer.Write(lastApplied.Term.Value);

            _state = SnapshotFileState.Initialized;

            _snapshotFileStream = stream;
        }

        public void Save()
        {
            switch (_state)
            {
                case SnapshotFileState.Start:
                    throw new InvalidOperationException("Попытка сохранения неинициализированного файла снапшота");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Нельзя повторно сохранить файл снапшота: работа с файлом закончена");
                case SnapshotFileState.Initialized:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            _parent._snapshotFileStream?.Close();

            // Флашить не надо, это сделает объект файла
            _tempSnapshotFile.Commit();

            _parent._snapshotFileStream = _snapshotFileStream;

            _state = SnapshotFileState.Finished;

            _snapshotFileStream = null!;
            _parent = null!;
            _tempSnapshotFile = null!;
        }

        public void Discard()
        {
            switch (_state)
            {
                case SnapshotFileState.Finished:
                    return;
                case SnapshotFileState.Start:
                case SnapshotFileState.Initialized:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            _state = SnapshotFileState.Finished;

            _tempSnapshotFile.Delete();

            _tempSnapshotFile = null!;
            _snapshotFileStream = null!;
        }

        public void WriteSnapshot(ISnapshot snapshot, CancellationToken token)
        {
            switch (_state)
            {
                case SnapshotFileState.Start:
                    throw new InvalidOperationException("Попытка записи снапшота в неинициализированный файл");
                case SnapshotFileState.Finished:
                    throw new InvalidOperationException(
                        "Нельзя записывать данные в файл снапшота: работа с файлом закончена");
                case SnapshotFileState.Initialized:
                    break;
                default:
                    throw new InvalidEnumArgumentException(nameof(_state), ( int ) _state, typeof(SnapshotFileState));
            }

            // Наверное, надо добавить еще одно состояние,
            // чтобы предотвратить повторную запись снапшота или нового
            snapshot.WriteTo(_snapshotFileStream, token);
        }
    }
}