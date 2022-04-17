ARG PYTHON_IMAGE_NAME=3.6.15-buster
FROM python:${PYTHON_IMAGE_NAME}

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

WORKDIR /app

COPY setup.py requirements.txt README.md ./

RUN pip install -r requirements.txt

RUN pip install -e ./

COPY ./ ./