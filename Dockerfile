FROM python:3.8.4-alpine

COPY ./ ./

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "ga.py"]
ENV DD_APP_KEY DD_API_KEY
