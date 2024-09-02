FROM apache/airflow:slim-latest
# syntax=docker/dockerfile:1

#FROM python:3.12
#FROM python:alpine

WORKDIR /app

COPY requirements.txt requirements.txt
COPY ./scipts/entrypoint.sh entrypoint.sh
RUN python -m pip install --upgrade pip
RUN pip install --upgrade -r requirements.txt

RUN chmod +x ./file-if-any/entrypoint.sh

COPY . .

CMD [ "python3", "-m" , "run", "--host=0.0.0.0"]