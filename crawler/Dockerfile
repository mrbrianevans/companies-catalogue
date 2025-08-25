FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY crawl_sftp.py .

CMD ["python", "crawl_sftp.py"]