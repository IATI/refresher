FROM python:3.11-slim-bookworm
RUN apt-get update && apt install -y \
    gcc \
    libpq-dev
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
ENTRYPOINT ["/usr/local/bin/python", "/code/handler.py"]
