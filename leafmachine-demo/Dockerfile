FROM python:3.12-slim

WORKDIR /code

RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

COPY leafmachine-demo/requirements.txt .

RUN pip install -r requirements.txt

COPY shared/ shared/

COPY leafmachine-demo/main.py .      

USER 1001

CMD [ "python", "main.py" ]