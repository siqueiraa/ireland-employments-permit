# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install git and dependencies
RUN apt-get update && apt-get install -y git

COPY requirements.txt .

RUN /bin/bash -c "pip install --upgrade pip"
RUN /bin/bash -c "pip install -r requirements.txt" && rm requirements.txt

RUN git clone https://github.com/siqueiraa/ireland-employments-permit.git .

# Start the data processing script
CMD ["python", "run_all.py"]