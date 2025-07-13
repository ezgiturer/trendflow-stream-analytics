FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code
COPY dashboard/ ./dashboard/

# Expose port (CapRover will route traffic here)
EXPOSE 3000

# Start Streamlit
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=3000", "--server.address=0.0.0.0"]