FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Streamlit app
COPY dashboard/ ./dashboard/

# Expose the port Streamlit will use
EXPOSE 8501

# Run the actual app
CMD ["streamlit", "run", "dashboard/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]