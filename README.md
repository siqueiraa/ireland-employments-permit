# Ireland Employments Permit

This project automates the process of downloading, processing, and providing employment permit data for Ireland.

## Setup Instructions

### Prerequisites

- Docker
- Docker Compose (for local setup)
- Kubernetes (for deployment on a cluster)
- Airflow

### Local Setup with Docker Compose

1. Clone the repository:
    ```bash
    git clone https://github.com/siqueiraa/ireland-employments-permit.git
    cd ireland-employments-permit
    ```

2. Build the Docker image:
    ```bash
    docker build -t siqueiraa/ireland-employments-permit:latest .
    ```

3. Run the services:
    ```bash
    docker-compose up -d
    ```

4. Access the application:
    - The REST API will be available at `http://localhost:4000/data`
    - Airflow UI will be available at `http://localhost:8080`

### Kubernetes Setup

1. Apply the Kubernetes manifests:
    ```bash
    kubectl apply -f k8s-deployment.yaml
    ```

2. Ensure the services are running:
    ```bash
    kubectl get pods
    kubectl get services
    ```

3. Access the application:
    - The REST API will be available at `http://<node-ip>:<node-port>/data`
    - Airflow UI will be available at `http://<node-ip>:<node-port>`

### Airflow Setup

#### Initializing Airflow

1. Initialize the Airflow database:
    ```bash
    airflow db init
    ```

2. Create an Airflow user:
    ```bash
    airflow users create --username admin --firstname FIRST_NAME --lastname LAST_NAME --role Admin --email admin@example.com
    ```

#### DAGs

The DAGs are defined in the `dags/` folder. The primary DAG is `check_and_run_processor_dag.py`, which checks for content changes on the specified URL and triggers the data processing if changes are detected.

### Contact

For any inquiries or support, please reach out via [LinkedIn](https://www.linkedin.com/in/rafael-siqueiraa/).

