using JobQueue.Core;

namespace JobQueue.Serialization.Tests;

public class JobQueueEqualityComparer : IEqualityComparer<IJobQueue>
{
    public static readonly JobQueueEqualityComparer Instance = new();

    public bool Equals(IJobQueue? x, IJobQueue? y)
    {
        return x is not null
            && y is not null
            && x.Name.Equals(y.Name)
            && MetadataEquals(x.Metadata, y.Metadata)
            && StoredDataEquals(x.GetAllData(), y.GetAllData());
    }

    public int GetHashCode(IJobQueue obj)
    {
        return HashCode.Combine(obj.Name, obj.Count, obj.Metadata);
    }

    private static bool MetadataEquals(IJobQueueMetadata first, IJobQueueMetadata second)
    {
        return first.QueueName.Equals(second.QueueName)
            && first.Count == second.Count
            && first.HasMaxSize == second.HasMaxSize
            && first.MaxSize == second.MaxSize;
    }

    private static bool StoredDataEquals(IReadOnlyCollection<(long, byte[])> first,
                                         IReadOnlyCollection<(long, byte[])> second)
    {
        if (first.Count != second.Count)
        {
            return false;
        }

        var x = first.GroupBy(x => x.Item1, x => x.Item2)
                     .OrderBy(x => x.Key)
                     .ToList();
        var y = second.GroupBy(z => z.Item1, z => z.Item2)
                      .OrderBy(z => z.Key)
                      .ToList();
        if (x.Count != y.Count)
        {
            return false;
        }

        foreach (var (left, right) in x.Zip(y))
        {
            if (left.Key != right.Key)
            {
                return false;
            }

            if (!StoredPayloadGroupEquals(left.ToArray(), right.ToArray()))
            {
                return false;
            }
        }

        return true;
    }


    private static bool StoredPayloadGroupEquals(byte[][] left, byte[]?[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }
        // Простая логика сравнения 2 наборов массивов работая с ними как с множествами:
        // для каждого массива из левой группы ищем соответствующий массив в правой группе

        // Проходимся по каждому массиву из левой группы 
        // и пытаемся найти массив из правой группы с ровно такими же элементами.
        // Если нашелся, то в правом обнуляем его на этой позиции (больше не используем), 
        // в противном случае соответсвующий массив не был найден, значит множества не равны
        foreach (var required in left)
        {
            var found = false;
            for (int j = 0; j < right.Length; j++)
            {
                if (right[j] is null)
                {
                    continue;
                }

                if (right[j]!.SequenceEqual(required))
                {
                    right[j] = null;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        return Array.TrueForAll(right, a => a is null);
    }
}