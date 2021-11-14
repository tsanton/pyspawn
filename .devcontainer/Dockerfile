ARG VARIANT="3.9"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# [Choice] Node.js version: none, lts/*, 16, 14, 12, 10
ARG NODE_VERSION="none"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi


RUN apt-get update -qq && apt-get install -y \
        curl  \
        openssl\
        gcc \
        libc-dev \
        unixodbc \
        unixodbc-dev \
        && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB3E94ADBE1229CF \
        && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
        && apt-get update -qq \
        && ACCEPT_EULA=y apt-get install -y  \
        msodbcsql17 \
        mssql-tools \
        && apt-get clean -y
 
COPY setup.py pyproject.toml requirements.txt ./

RUN pip install -r requirements.txt