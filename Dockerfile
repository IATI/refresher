FROM python:3.8-slim-bullseye
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
ENTRYPOINT ["/usr/local/bin/python", "/code/handler.py"]
