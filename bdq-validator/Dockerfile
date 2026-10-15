FROM python:3.12-slim

WORKDIR /app
COPY bdq-validator/requirements.lock .
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.lock \
    && useradd --create-home --uid 1001 mas
COPY bdq-validator/main.py bdq-validator/bdq.py bdq-validator/annotations.py bdq-validator/gbif.py ./
USER 1001

ENTRYPOINT ["python", "main.py"]
CMD ["--rabbitmq"]
