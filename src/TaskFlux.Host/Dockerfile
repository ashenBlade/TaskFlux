FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
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
