# Ireland Employments Permit

This project automates the process of downloading, processing, and providing employment permit data for Ireland by uploading it to Google Sheets.

## Setup Instructions

### Prerequisites

- Docker
- Kubernetes

### Local Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/siqueiraa/ireland-employments-permit.git
    cd ireland-employments-permit
    ```

2. Install dependencies:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3. Run the main script:
    ```bash
    python run_all.py
    ```

### Kubernetes Setup

1. Create a Kubernetes Secret for your Google service account credentials:
    ```bash
    kubectl create secret generic google-credentials --from-file=credentials.json=./credentials.json
    ```

2. Build and push the Docker image:
    ```bash
    docker buildx build --platform linux/arm64 -t siqueiraa/ireland-employments-permit:latest -f Dockerfile . --push --no-cache
    ```

3. Apply the Kubernetes manifests:
    ```bash
    kubectl apply -f k8s-cronjob.yaml
    ```

4. Ensure the CronJob is running:
    ```bash
    kubectl get cronjobs
    ```

### Dashboard

You can view the data on the [Google Looker Studio Dashboard](https://lookerstudio.google.com/reporting/a341875a-d2ed-4e36-b8f7-08edd883e4d4/page/htLqD).

### Contact

For any inquiries or support, please reach out via [LinkedIn](https://www.linkedin.com/in/rafael-siqueiraa/).
