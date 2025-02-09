FROM python:3.8-slim-buster

WORKDIR /app

ENV OPENAI_API_KEY="sk-1234567890abcdef1234567890abcdef"

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "flask", "run", "--host=0.0.0.0"]