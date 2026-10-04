FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ .
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
# every service uses the same image, compose picks the module
CMD ["uvicorn", "student:app", "--host", "0.0.0.0", "--port", "8000"]
