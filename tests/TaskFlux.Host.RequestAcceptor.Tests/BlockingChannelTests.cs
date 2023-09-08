using FluentAssertions;
using Xunit;

namespace TaskFlux.Host.RequestAcceptor.Tests;

// ReSharper disable AccessToDisposedClosure
[Trait("Category", "BusinessLogic")]
public class BlockingChannelTests
{
    [Fact(Timeout = 1000)]
    public async Task Write__ЕдинственноеЗначение__ДолженЗаписатьЗначениеВОчередь()
    {
        using var cts = new CancellationTokenSource();
        var channel = new BlockingChannel<int>();
        var result = new List<int>();
        var task = Task.Run(() =>
        {
            // ReSharper disable once AccessToDisposedClosure
            var x = cts;
            foreach (var i in channel.ReadAll(x.Token))
            {
                result.Add(i);
                x.Cancel();
            }
        }, cts.Token);
        channel.Write(1);
        await task;
        result.Should()
              .ContainSingle(i => i == 1, "единственное записанное значение - 1");
    }

    [Fact(Timeout = 1000)]
    public async Task Write__НесколькоЗначений__ДолженЗаписатьВсеЗначения()
    {
        using var cts = new CancellationTokenSource();
        var channel = new BlockingChannel<int>();
        var result = new List<int>();
        var values = new[] {1, 3, 5, 23453, 43, -142, 464, 53687, 65633333, int.MaxValue};
        var task = Task.Run(() =>
        {
            var x = cts;
            var count = values.Length;
            var i = 0;
            foreach (var value in channel.ReadAll(x.Token))
            {
                result.Add(value);
                i++;
                if (i == count)
                {
                    x.Cancel();
                }
            }
        }, cts.Token);

        foreach (var value in values)
        {
            channel.Write(value);
        }

        await task;

        result.Should()
              .ContainInOrder(values, "все значения записывались последовательно в одном потоке");
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task Write__КогдаНесколькоЗначенийЗаписываютсяПараллельно__ДолженЗаписатьВсе(int parallelCount)
    {
        using var cts = new CancellationTokenSource();
        var channel = new BlockingChannel<int>();
        var result = new List<int>();
        var size = 100;
        var values = Enumerable.Range(0, parallelCount * size)
                               .Chunk(size)
                               .ToArray();
        var task = Task.Run(() =>
        {
            var x = cts;
            var totalCount = parallelCount * size;
            var i = 0;
            foreach (var value in channel.ReadAll(x.Token))
            {
                result.Add(value);
                i++;
                if (i == totalCount)
                {
                    x.Cancel();
                }
            }
        }, cts.Token);

        await Task.WhenAll(values.Select(array => Task.Run(() =>
                                  {
                                      foreach (var i in array)
                                      {
                                          channel.Write(i);
                                      }
                                  }, cts.Token))
                                 .Prepend(task));

        var expected = values.SelectMany(x => x).ToArray();

        // Не важно в каком порядке - это проблема многопоточности
        Assert.Equal(expected.ToHashSet(), result.ToHashSet());
    }

    [Fact]
    public async Task ReadAll__КогдаТокенОтменен__ДолженВернутьОставшиесяЭлементы()
    {
        using var cts = new CancellationTokenSource();
        var channel = new BlockingChannel<int>();
        var result = new List<int>();
        var values = Enumerable.Range(0, 1000).ToArray();
        var middle = values.Length / 2;
        var writtenAll = new TaskCompletionSource();
        var readTask = Task.Run(async () =>
        {
            foreach (var (value, i) in channel.ReadAll(cts.Token).Select((value, i) => ( value, i )))
            {
                result.Add(value);
                if (i == middle)
                {
                    await writtenAll.Task;
                }
            }
        }, cts.Token);
        var writeTask = Task.Run(() =>
        {
            foreach (var value in values)
            {
                channel.Write(value);
            }

            cts.Cancel();
            writtenAll.SetResult();
        }, cts.Token);

        await Task.WhenAll(readTask, writeTask);

        result.Should()
              .ContainInOrder(values, "все значения записывались последовательно в одном потоке");
    }
}