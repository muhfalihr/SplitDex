FROM python:3.10-slim-bullseye

WORKDIR /app

COPY source/. /app/

RUN apt-get update && apt install -y \
    gcc \
    libpq5 \
    libpq-dev \
    && apt-get clean

RUN pip install --upgrade pip && pip install --no-cache-dir -r /app/requirements.txt

CMD ["python", "main.py"]