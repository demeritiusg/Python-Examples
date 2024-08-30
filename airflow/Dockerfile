#FROM apache/airflow
# syntax=docker/dockerfile:1

FROM python:3.12

WORKDIR /app

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --upgrade -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "run", "--host=0.0.0.0"]