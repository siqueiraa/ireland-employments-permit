# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install git and dependencies
RUN apt-get update && apt-get install -y git

# Clone the repository
RUN git clone https://github.com/siqueiraa/ireland-employments-permit.git .

# Create and activate virtual environment
RUN python -m venv .venv

# Install Python dependencies within the virtual environment
RUN /bin/bash -c "source .venv/bin/activate && pip install --no-cache-dir -r requirements.txt"

# Install Airflow and Kubernetes provider within the virtual environment
RUN /bin/bash -c "source .venv/bin/activate && pip install apache-airflow[cncf.kubernetes]"

# Copy DAGs to the Airflow DAGs directory
COPY dags/ /app/dags/

# Set the Airflow home directory and create the configuration file
ENV AIRFLOW_HOME=/app/airflow
RUN mkdir -p ${AIRFLOW_HOME}

# Initialize Airflow default configuration
RUN /bin/bash -c "source .venv/bin/activate && airflow db init && airflow config save ${AIRFLOW_HOME}/airflow.cfg"

# Append DAGs folder configuration to airflow.cfg
RUN echo -e "\n[core]\ndags_folder = /app/dags" >> ${AIRFLOW_HOME}/airflow.cfg

# Create Airflow user
ENV AIRFLOW_USER=admin
ENV AIRFLOW_FIRSTNAME=FIRST_NAME
ENV AIRFLOW_LASTNAME=LAST_NAME
ENV AIRFLOW_ROLE=Admin
ENV AIRFLOW_EMAIL=admin@example.com
ENV AIRFLOW_PASSWORD=admin

RUN /bin/bash -c "source .venv/bin/activate && airflow users create --username $AIRFLOW_USER --firstname $AIRFLOW_FIRSTNAME --lastname $AIRFLOW_LASTNAME --role $AIRFLOW_ROLE --email $AIRFLOW_EMAIL --password $AIRFLOW_PASSWORD"

# Expose the ports for Airflow webserver and Flask app
EXPOSE 8080 4000

# Start Airflow scheduler, webserver, and the Flask app
CMD /bin/bash -c "source .venv/bin/activate && airflow scheduler & airflow webserver --port 8080 & python app.py"
