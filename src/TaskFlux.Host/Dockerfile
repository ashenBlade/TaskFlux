FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/TaskFlux.Host/TaskFlux.Host.csproj", "src/TaskFlux.Host/"]
COPY ["src/TaskFlux.Transport/TaskFlux.Transport.Http/TaskFlux.Transport.Http.csproj", "src/TaskFlux.Transport/TaskFlux.Transport.Http/"]
COPY ["src/Consensus/TaskFlux.Consensus/TaskFlux.Consensus.csproj", "src/Consensus/TaskFlux.Consensus/"]
COPY ["src/TaskFlux.Core/TaskFlux.Core.csproj", "src/TaskFlux.Core/"]
COPY ["src/TaskFlux.PriorityQueue/TaskFlux.PriorityQueue.csproj", "src/TaskFlux.PriorityQueue/"]
COPY ["src/TaskFlux.Utils/TaskFlux.Utils.csproj", "src/TaskFlux.Utils/"]
COPY ["src/TaskFlux.Application/TaskFlux.Application.csproj", "src/TaskFlux.Application/"]
COPY ["src/TaskFlux.Persistence/TaskFlux.Persistence.csproj", "src/TaskFlux.Persistence/"]
COPY ["src/TaskFlux.Transport/TaskFlux.Transport.Tcp/TaskFlux.Transport.Tcp.csproj", "src/TaskFlux.Transport/TaskFlux.Transport.Tcp/"]
COPY ["src/TaskFlux.Network/TaskFlux.Network.csproj", "src/TaskFlux.Network/"]
COPY ["src/TaskFlux.Transport/TaskFlux.Transport.Grpc/TaskFlux.Transport.Grpc.csproj", "src/TaskFlux.Transport/TaskFlux.Transport.Grpc/"]
RUN dotnet restore "src/TaskFlux.Host/TaskFlux.Host.csproj"
COPY . .
WORKDIR "/src/src/TaskFlux.Host"
RUN dotnet build "TaskFlux.Host.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "TaskFlux.Host.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TaskFlux.Host.dll"]
