using Xunit.Sdk;

namespace JobQueue.Core.Tests;

[Trait("Category", "BusinessLogic")]
public class QueueNameParserTests
{
    private static void AssertBase(string name)
    {
        var q = QueueNameParser.Parse(name);
        Assert.Equal(q.Name, name);
    }

    private static void AssertErrorBase(string name)
    {
        var ex = Record.Exception(() => QueueNameParser.Parse(name));
        Assert.NotNull(ex);
    }

    [Fact]
    public void Parse__КогдаСтрокаПуста__ДолженСпарсить()
    {
        AssertBase("");
    }

    [Fact]
    public void Parse__КогдаНазваниеИз1РазрешенногоСимвола__ДолженСпарситьКорректно()
    {
        foreach (var ch in QueueNameParser.AllowedCharacters)
        {
            AssertBase(ch.ToString());
        }
    }

    [Theory]
    [InlineData("queue")]
    [InlineData("messages:main:1")]
    [InlineData("messages.main.1")]
    [InlineData("GRAPH.EVENTS>0")]
    [InlineData("GRAPH.EVENTS>5.30")]
    [InlineData("_")]
    [InlineData("-")]
    [InlineData("---hello---world---!")]
    public void Parse__КогдаСтрокаДлиныНеБольше255СДопустимымиСимволами__ДолженСпарситьКорректно(string name)
    {
        AssertBase(name);
    }

    [Fact]
    public void Parse__КогдаСтрокаДлиной255__ДолженСпарситьКорректно()
    {
        AssertBase(string.Create(255, 'a', (span, c) => span.Fill(c)));
    }

    [Fact]
    public void Parse__КогдаСтрокаИз1НедопустимогоСимвола__ДолженСпарситьСОшибкой()
    {
        var i = ( char ) 0;
        while (i < ( char ) 33)
        {
            AssertErrorBase(i.ToString());
            i++;
        }

        i = ( char ) 127;
        while (i != ( char ) 0)
        {
            AssertErrorBase(i.ToString());
            i++;
        }
    }

    [Theory]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(258)]
    [InlineData(300)]
    public void Parse__КогдаДлинаСтрокиБольше255__ДолженСпарситьСОшибкой(int length)
    {
        var str = string.Create(length, 'a', (span, c) => span.Fill(c));
        AssertErrorBase(str);
    }

    private static void AssertBase(ReadOnlySpan<byte> bytes, string expected)
    {
        var name = QueueNameParser.Parse(bytes);
        Assert.Equal(expected, name);
    }

    [Fact]
    public void Parse__КогдаПереданПустойМассивБайт__ДолженСпарситьВОчередьПоУмолчанию()
    {
        AssertBase(Array.Empty<byte>(), QueueName.DefaultName);
    }

    [Fact]
    public void Parse__КогдаПереданМассивИз1ПравильногоСимвола__ДолженСпарситьПравильно()
    {
        for (var i = 0; i < QueueNameParser.AllowedCharacters.Length; i++)
        {
            var ch = QueueNameParser.AllowedCharacters[i];
            var expected = ch.ToString();
            try
            {
                AssertBase(new[] {( byte ) ch}, expected);
            }
            catch (EqualException e)
            {
                throw new EqualException(expected, e.Actual, 0, i);
            }
        }
    }


    private byte[] GetBytes(string str) => QueueNameParser.Encoding.GetBytes(str);

    [Theory]
    [InlineData(QueueName.DefaultName)]
    [InlineData("hello")]
    [InlineData("world!!!")]
    [InlineData("!~")]
    [InlineData("asdfJKSDFGPNt34ASOIUW{4524E*WAESV{\":#^@!&2345W@$TJ$QQQVQWaergae")]
    [InlineData("asdfasdf")]
    [InlineData("default")]
    [InlineData("queue:1:test")]
    [InlineData("GRAPH_DEFAULT_1")]
    public void Parse__КогдаПередаетсяМассивИзПравильныхБайтов__ДолженСпарситьКорректно(string expected)
    {
        AssertBase(GetBytes(expected), expected);
    }

    private void AssertErrorBase(byte[] data)
    {
        var ex = Record.Exception(() => QueueNameParser.Parse(data));
        Assert.NotNull(ex);
    }

    [Fact]
    public void Parse__КогдаМассивИз1НедопустимогоСимвола__ДолженСпарситьСОшибкой()
    {
        byte d = 0;
        while (d < ( byte ) '!')
        {
            AssertErrorBase(new[] {d});
            d++;
        }

        d = ( ( byte ) '~' ) + 1;
        while (true)
        {
            AssertErrorBase(new[] {d});
            if (d == byte.MaxValue)
            {
                break;
            }

            d++;
        }
    }

    [Fact]
    public void Parse__КогдаВМассиве255Символов__ДолженСпарситьКорректно()
    {
        var b = new byte[255];
        Array.Fill(b, ( byte ) 'A');
        AssertBase(b, QueueNameParser.Encoding.GetString(b));
    }

    [Theory]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(300)]
    [InlineData(400)]
    public void Parse__КогдаВМассивеБольше255Символов__ДолженСпарситьСОшибкой(int length)
    {
        var b = new byte[length];
        Array.Fill(b, ( byte ) 'A');
        AssertErrorBase(b);
    }
}