# Start from an official Python image
FROM python:3.11-slim

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the requirements file first to leverage Docker's layer caching
COPY requirements.txt .

# Install all your Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# The command to run your server when the container starts.
# Because the WORKDIR is /app, Docker will look for app.py in that directory.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
