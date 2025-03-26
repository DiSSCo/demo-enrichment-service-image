FROM python:3.11-slim

WORKDIR /code

RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

COPY ontogpt-habitat/requirements.txt .

RUN pip install -r requirements.txt

COPY shared/ shared/

COPY ontogpt-habitat/main.py .      

USER 1001

CMD [ "python", "main.py" ]