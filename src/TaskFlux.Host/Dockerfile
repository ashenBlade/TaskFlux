FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /var/lib/tflux

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore "TaskFlux.Host/TaskFlux.Host.csproj"
WORKDIR "/src/TaskFlux.Host"
RUN dotnet build "TaskFlux.Host.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "TaskFlux.Host.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final

COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TaskFlux.Host.dll"]
