using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using JobQueue.Core.Exceptions;

[assembly: InternalsVisibleTo("JobQueue.Core.Tests")]
[assembly: InternalsVisibleTo("TaskFlux.Commands.Serialization.Tests")]

namespace JobQueue.Core;

/// <summary>
/// Объект представляющий название очереди, удовлетворяющее бизнес-логике
/// </summary>
public struct QueueName
{
    public static QueueName Default => new(DefaultName);
    public const string DefaultName = "";
    
    public bool IsDefaultQueue => Name == DefaultName;
    public string Name { get; }
    
    internal QueueName(string name)
    {
        Name = name;
    }
    
    public QueueName()
    {
        Name = DefaultName;
    }

    public static implicit operator string(QueueName queueName)
    {
        return queueName.Name;
    }

    public override string ToString()
    {
        return $"QueueName({Name})";
    }

    public override int GetHashCode()
    {
        return Name.GetHashCode();
    }
}