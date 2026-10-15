FROM python:3.13-slim

WORKDIR /code

RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

COPY ruler_detection/requirements.txt .

RUN pip install -r requirements.txt

COPY shared/ shared/

COPY ruler_detection/main.py .

USER 1001

CMD [ "python", "main.py" ]
