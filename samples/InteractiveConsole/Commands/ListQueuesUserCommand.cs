using TaskFlux.Client;

namespace InteractiveConsole.Commands;

public class ListQueuesUserCommand : UserCommand
{
    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        var result = await client.GetAllQueuesInfoAsync(token);
        Console.WriteLine($"Очереди:");
        foreach (var metadata in result)
        {
            Console.WriteLine($" - Название: {metadata.QueueName.Name}");
            Console.WriteLine($"   Размер: {metadata.Count}");
            if (metadata.Policies.Count > 0)
            {
                Console.WriteLine($"   Политики очереди: ");
                foreach (var (key, value) in metadata.Policies)
                {
                    var (processedKey, processedValue) = GetPolicyName(key, value);
                    Console.WriteLine($"    - {processedKey}: {processedValue}");
                }
            }

            Console.WriteLine();
        }
    }

    private static (string Key, string Value) GetPolicyName(string key, string value)
    {
        return key switch
               {
                   "max-queue-size"   => ( "Максимальный размер очереди", GetHumanReadableSize(value) ),
                   "priority-range"   => ( "Диапазон ключей", GetPriorityRangeValue(value) ),
                   "max-payload-size" => ( "Максимальный размер сообщения", GetHumanReadableSize(value) ),
                   _                  => ( key, value )
               };

        static string GetPriorityRangeValue(string range)
        {
            var x = range.Split(' ');
            var min = long.Parse(x[0]);
            var max = long.Parse(x[1]);
            return $"Мин: {min}, Макс: {max}";
        }

        static string GetHumanReadableSize(string value)
        {
            var sizeBytes = int.Parse(value);
            if (sizeBytes < 1024)
            {
                return $"{value} байт";
            }

            var kb = ( ( double ) sizeBytes ) / 1024;
            if (kb < 1024)
            {
                return $"{Math.Round(kb, 1)} Кб";
            }

            var mb = kb / 1024;
            return $"{Math.Round(mb, 1)} Мб";
        }
    }
}