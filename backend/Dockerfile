FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY run.py ./run.py
RUN useradd --create-home --uid 10001 codexin && mkdir -p /app/data/raw-events && chown -R codexin:codexin /app
USER codexin
EXPOSE 8000
CMD ["python", "run.py"]
