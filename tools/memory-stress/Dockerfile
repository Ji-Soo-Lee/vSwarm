# Use a lightweight Python base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the Python script into the container
COPY memory_stress.py /app/memory_stress.py

# Install any dependencies (none required here)
RUN pip install --no-cache-dir argparse

# Define the command to run the script
ENTRYPOINT ["python", "memory_stress.py"]
