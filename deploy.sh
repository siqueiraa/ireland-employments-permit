docker buildx build --platform linux/arm64 -t siqueiraa/ireland-employments-permit:latest -f Dockerfile . --push --no-cache

DIFF_OUTPUT=$(kubectl diff -f k8s-deployment.yaml)

if [ -n "$DIFF_OUTPUT" ]; then
    echo "Differences detected in the manifest. Applying changes and restarting the deployment..."
    
    kubectl apply -f k8s-deployment.yaml

    sleep 5

    kubectl get pods
else
    kubectl rollout restart deployment ireland-employments-permit
    echo "No differences detected in the manifest. Restarting."
fi

