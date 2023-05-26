FROM python:3.9

RUN mkdir -p /app/data
VOLUME /app/data
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "elasticSearchHandler.py"]
