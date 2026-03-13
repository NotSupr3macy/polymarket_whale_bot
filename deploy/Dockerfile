FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Create logs directory
RUN mkdir -p logs

# Default: dry run. Override with docker run args.
# Live: docker run -d --env-file .env whale-bot python3 cli.py --live
CMD ["python3", "cli.py"]
