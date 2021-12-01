#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

ENV DB_USER=
ENV DB_PASSWORD=
ENV DB_ENDPOINT=
ENV DB_PORT=
ENV DB_DB=
ARG CERT_PASSW=

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["WeatherAPI.csproj", "."]
RUN dotnet restore "./WeatherAPI.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build "WeatherAPI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "WeatherAPI.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .

ENTRYPOINT ["dotnet", "WeatherAPI.dll"]