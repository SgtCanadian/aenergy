FROM python:3.12-alpine

ENV AENERGY_API_KEY ''
ENV AENERGY_DB_URL ''

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./

CMD ["python", "./main.py"]