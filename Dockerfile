FROM python:3.8
RUN apt-get update && apt-get -y install cron
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
COPY refresh-cron /etc/cron.d/refresh-cron
RUN chmod 0644 /etc/cron.d/refresh-cron
RUN crontab /etc/cron.d/refresh-cron
CMD printenv > /etc/environment