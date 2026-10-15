FROM python:3.12-slim

WORKDIR /app
COPY bdq-validator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 1001 mas
COPY bdq-validator/main.py bdq-validator/bdq.py bdq-validator/annotations.py bdq-validator/gbif.py ./
USER 1001

ENTRYPOINT ["python", "main.py"]
CMD ["--rabbitmq"]
