FROM python:3.13-slim

WORKDIR /code

RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

COPY herbarium-sheet-plant-organ-segmentation/requirements.txt .

RUN pip install -r requirements.txt

COPY shared/ shared/

COPY herbarium-sheet-plant-organ-segmentation/main.py .      

USER 1001

CMD [ "python", "main.py" ]