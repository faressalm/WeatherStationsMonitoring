FROM python:3.9
WORKDIR /app
COPY elasticSearchHandler.py .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "elasticSearchHandler.py"]
