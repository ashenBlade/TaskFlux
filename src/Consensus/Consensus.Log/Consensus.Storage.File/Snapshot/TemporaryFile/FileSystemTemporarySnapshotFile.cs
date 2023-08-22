using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Consensus.Storage.File.Snapshot;

/// <summary>
/// Реализация объекта временного файла снапшота,
/// работающая с реальной файловой системой
/// </summary>
public class FileSystemTemporarySnapshotFile : ITemporarySnapshotFile
{
    /// <summary>
    /// Файл снапшота, который мы собираемся заменить
    /// </summary>
    private FileInfo _snapshotFile;

    /// <summary>
    /// Временный файл снапшота
    /// </summary>
    private FileInfo _temporaryFile;

    /// <summary>
    /// Поток, представляющий временный файл
    /// </summary>
    private Stream? _temporaryFileStream;

    /// <summary>
    /// Текущее состояние объекта файла снапшота
    /// </summary>
    private TemporaryFileState _currentState = TemporaryFileState.Start;

    public FileSystemTemporarySnapshotFile(FileInfo snapshotFile, FileInfo temporaryFile)
    {
        _snapshotFile = snapshotFile;
        _temporaryFile = temporaryFile;
    }

    public Stream Open()
    {
        switch (_currentState)
        {
            case TemporaryFileState.Start or TemporaryFileState.Created:
                break;
            case TemporaryFileState.Committed:
                throw new InvalidOperationException("Нельзя открывать файл снапшота заново: файл уже закоммичен");
            case TemporaryFileState.Discarded:
                throw new InvalidOperationException("Нельзя отбрасывать файл снапшота заново: файл уже закомичен");
            default:
                ThrowInvalidState();
                break;
        }

        if (_temporaryFileStream is null)
        {
            Stream stream = _temporaryFile.Open(FileMode.Create);

            // Данные на диск надо сбрасывать как можно реже, 
            // т.к. сброс на диск операция дорогая,
            // а время создания снапшота должно быть как можно короче 
            stream = new BufferedStream(stream);
            _temporaryFileStream = stream;

            _currentState = TemporaryFileState.Created;
        }

        return _temporaryFileStream;
    }


    public void Commit()
    {
        switch (_currentState)
        {
            case TemporaryFileState.Start:
                throw new InvalidOperationException("Нельзя закоммитить файл снапшота: файл еще не был создан");
            case TemporaryFileState.Created:
                break;
            case TemporaryFileState.Committed:
                throw new InvalidOperationException("Нельзя закоммитить файл снапшота повторно");
            case TemporaryFileState.Discarded:
                throw new InvalidOperationException("Нельзя закоммитить файл снапшота: файл был отброшен");
            default:
                ThrowInvalidState();
                return;
        }

        Debug.Assert(_temporaryFileStream is not null,
            "Был вызван Commit метод временного файла снапшота, но поток файла не был иницилазирован");

        _temporaryFileStream.Flush();

        // Переименовываем и при необходимости перезаписываем
        _temporaryFile.MoveTo(_snapshotFile.FullName, true);

        _temporaryFile = null!;
        _temporaryFileStream = null;
        _snapshotFile = null!;

        _currentState = TemporaryFileState.Committed;
    }

    public void Delete()
    {
        switch (_currentState)
        {
            case TemporaryFileState.Start or TemporaryFileState.Created:
                break;
            case TemporaryFileState.Committed:
                throw new InvalidOperationException("Нельзя отбросить файл снапшота: файл был закоммичен");
            case TemporaryFileState.Discarded:
                return;
            default:
                ThrowInvalidState();
                return;
        }

        _currentState = TemporaryFileState.Discarded;

        if (_temporaryFile.Exists)
        {
            _temporaryFileStream?.Close();
            _temporaryFile.Delete();
        }

        _temporaryFile = null!;
        _temporaryFileStream = null;
        _snapshotFile = null!;
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private void ThrowInvalidState()
    {
        throw new InvalidEnumArgumentException(nameof(_currentState), ( int ) _currentState,
            typeof(TemporaryFileState));
    }

    /// <summary>
    /// Статус временного файла снапшота.
    /// </summary>
    private enum TemporaryFileState
    {
        /// <summary>
        /// Первоначальный статус. Файл еще не был создан
        /// </summary>
        Start = 0,

        /// <summary>
        /// Файл открыт и работа с ним идет
        /// </summary>
        Created = 1,

        /// <summary>
        /// Файл успешно сброшен на диск.
        /// </summary>
        Committed = 2,

        /// <summary>
        /// Файл снапшота отброшен
        /// </summary>
        Discarded = 3,
    }
}