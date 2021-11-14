FROM mcr.microsoft.com/mssql/server AS base

ARG ACCEPT_EULA
ARG SA_PASSWORD 

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=10 \
    CMD /opt/mssql-tools/bin/sqlcmd -S . -U sa -P ${SA_PASSWORD} -d master -q "CREATE DATABASE SqlServerTests"