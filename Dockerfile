# MOEX Screener v3.1 — Docker image
# Build: docker build -t moex-screener .
# Run:   docker run --rm -it -v $(pwd)/robot_db:/app/robot_db moex-screener

FROM python:3.13-slim

WORKDIR /app

# Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application
COPY moex_screener_v3_1.py .

# Config (optional override)
COPY .env.example ./.env

# Robot DB volume (persisted between runs)
VOLUME ["/app/robot_db"]

# Timezone for correct Moscow timestamps
ENV TZ=Etc/UTC
ENV ROBOT_DB_DIR=/app/robot_db

EXPOSE 8080  # reserved for future alert webhooks

ENTRYPOINT ["python", "moex_screener_v3_1.py"]
CMD ["--rest"]
