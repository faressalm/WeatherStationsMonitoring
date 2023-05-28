FROM python:3.9
COPY . /usr/app/
WORKDIR /usr/app
RUN pip install -r requirements.txt
CMD sleep 30 && python elasticSearchHandler.py
