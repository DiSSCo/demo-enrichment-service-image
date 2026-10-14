FROM python:3.12-slim

WORKDIR /app
COPY bdq-test/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 1001 mas
COPY bdq-test/main.py bdq-test/bdq.py bdq-test/annotations.py ./
USER 1001

ENTRYPOINT ["python", "main.py"]
CMD ["--rabbitmq"]
