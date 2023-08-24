namespace JobQueue.Core.TestHelpers;

public static class QueueNameHelpers
{
    public static QueueName CreateRandomQueueName()
    {
        var length = Random.Shared.Next(0, QueueNameParser.MaxNameLength);
        return new QueueName(string.Create(length, Random.Shared, (span, random) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = QueueNameParser.AllowedCharacters[
                    random.Next(0, QueueNameParser.AllowedCharacters.Length)];
            }
        }));
    }
}