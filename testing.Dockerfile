FROM python:3.9.0


WORKDIR /app


RUN apt-get update -qq && apt-get install -y \
        curl  \
        openssl\
        && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB3E94ADBE1229CF \
        && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
        && apt-get update -qq \
        && ACCEPT_EULA=y apt-get install -y  \
        msodbcsql17 \
        mssql-tools \
        unixodbc-dev \
        && apt-get clean -y

COPY requirements.txt run_tests.sh ./

RUN pip install -r requirements.txt

COPY . .

RUN pip install -e ./

RUN chmod +x ./run_tests.sh