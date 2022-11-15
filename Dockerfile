FROM python:3.8-slim-bullseye
RUN apt-get update && apt install -y \
    gcc \
    libpq-dev
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
ENTRYPOINT ["/usr/local/bin/python", "/code/handler.py"]
