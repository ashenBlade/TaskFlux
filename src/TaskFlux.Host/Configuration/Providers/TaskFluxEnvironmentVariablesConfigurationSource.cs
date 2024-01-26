using System.Collections;
using Microsoft.Extensions.Configuration;
using TaskFlux.Host.Infrastructure;

namespace TaskFlux.Host.Configuration.Providers;

public class TaskFluxEnvironmentVariablesConfigurationSource : IConfigurationSource
{
    /// <summary>
    /// Префикс для переменных окружения с конфигурацией для приложения
    /// </summary>
    public const string Prefix = "TASKFLUX";

    private const string PrefixUnderscored = $"{Prefix}_";

    public IConfigurationProvider Build(IConfigurationBuilder builder)
    {
        return new EnvironmentVariablesProvider();
    }

    private class EnvironmentVariablesProvider : ConfigurationProvider
    {
        public override void Load()
        {
            var env = Environment.GetEnvironmentVariables();

            // Все аргументы передаются через 
            var data = new Dictionary<string, string?>();
            foreach (DictionaryEntry entry in env)
            {
                if (entry.Key.ToString() is not { } key || !key.StartsWith(PrefixUnderscored))
                {
                    continue;
                }

                data[NormalizeEnvironmentVariableName(key)] = entry.Value?.ToString();
            }

            Data = data;
        }

        private static string NormalizeEnvironmentVariableName(string name)
        {
            return string.Join(string.Empty,
                name.Remove(0, PrefixUnderscored.Length)
                    .Split('_')
                    .Select(StringExtensions.Capitalize));
        }
    }
}