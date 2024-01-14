namespace TaskFlux.Host.Infrastructure;

public static class StringExtensions
{
    public static string Capitalize(this string s) =>
        s switch
        {
            {Length: 0} => s,
            _ => string.Create(s.Length, s, static (span, str) =>
            {
                str.CopyTo(span);
                span[0] = char.ToUpperInvariant(span[0]);
                for (var i = 1; i < span.Length; i++)
                {
                    span[i] = char.ToLowerInvariant(span[i]);
                }
            })
        };
}