FROM python:3.12-slim
WORKDIR /code
COPY traittrail-specimen-observation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY traittrail-specimen-observation/main.py traittrail-specimen-observation/annotations.py traittrail-specimen-observation/contract.py ./
COPY traittrail-specimen-observation/schemas ./schemas
USER 1001:1001
CMD ["python", "main.py"]
