docker buildx build --platform linux/arm64 -t siqueiraa/ireland-employments-permit:latest -f Dockerfile . --push --no-cache

kubectl apply -f k8s-cronjob.yaml