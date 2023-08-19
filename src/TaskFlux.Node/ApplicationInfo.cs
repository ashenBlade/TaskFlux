using JobQueue.Core;
using TaskFlux.Core;

namespace TaskFlux.Node;

public class ApplicationInfo: IApplicationInfo
{
    public ApplicationInfo(QueueName defaultQueueName)
    {
        DefaultQueueName = defaultQueueName;
    }

    public Version Version => Constants.CurrentVersion;
    public QueueName DefaultQueueName { get; }

    public override string ToString()
    {
        return $"ApplicationInfo(Version = {Version.ToString()}, DefaultQueueName = {DefaultQueueName.Name})";
    }
}