using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.CommandLine;
using TaskFlux.Host.Infrastructure;

namespace TaskFlux.Host.Configuration.Providers;

public class TaskFluxCommandLineConfigurationSource : IConfigurationSource
{
    private readonly IEnumerable<string> _args;

    public TaskFluxCommandLineConfigurationSource(IEnumerable<string> args)
    {
        _args = args;
    }

    public IConfigurationProvider Build(IConfigurationBuilder builder)
    {
        return new CommandLineProvider(_args);
    }

    private class CommandLineProvider : CommandLineConfigurationProvider
    {
        public CommandLineProvider(IEnumerable<string> args, IDictionary<string, string>? switchMappings = null) : base(
            args, switchMappings)
        {
        }

        public override void Load()
        {
            base.Load();

            // Все что нам нужно сделать - преобразовать названия аргументов в каноничный вид
            var data = new Dictionary<string, string?>(Data.Count);
            foreach (var (key, value) in Data)
            {
                data[NormalizeCommandLineArgumentName(key)] = value;
            }

            Data = data;
        }

        /// <summary>
        /// Привести имя аргумента из командной строки в каноничный для конфигурации формат.
        /// Например:
        /// - sample-options -> SampleOptions
        /// - connection-delay -> ConnectionDelay
        /// </summary>
        /// <param name="name">Название аргумента командной строки</param>
        /// <returns>Нормализованное название аргумента</returns>
        private static string NormalizeCommandLineArgumentName(string name)
        {
            return string.Join(string.Empty,
                name.Split('-')
                    .Select(StringExtensions.Capitalize));
        }
    }
}