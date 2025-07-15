#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "Error: .env file not found"
    exit 1
fi

# Check if DEPLOYMENTS variable is set
if [ -z "$DEPLOYMENTS" ]; then
    echo "Error: DEPLOYMENTS variable not set in .env file"
    echo "Please add DEPLOYMENTS=deployment1,deployment2,deployment3 to your .env file"
    exit 1
fi

# Check if NAMESPACE variable is set
if [ -z "$NAMESPACE" ]; then
    echo "Error: NAMESPACE variable not set in .env file"
    echo "Please add NAMESPACE=your-namespace to your .env file"
    exit 1
fi

# Configuration for timeouts and retries
KUBECTL_TIMEOUT=${KUBECTL_TIMEOUT:-300}  # 5 minutes default
MAX_RETRIES=${MAX_RETRIES:-3}
RETRY_DELAY=${RETRY_DELAY:-5}
MAX_PARALLEL_COPIES=${MAX_PARALLEL_COPIES:-10}  # Maximum parallel copy operations

echo "Deployments to process: $DEPLOYMENTS"
echo "Namespace: $NAMESPACE"
echo "Kubectl timeout: ${KUBECTL_TIMEOUT}s"
echo "Max retries: $MAX_RETRIES"
echo "Max parallel copies: $MAX_PARALLEL_COPIES"

# Debug: Check if kubeconfig exists and kubectl works
echo ""
echo "=== DEBUGGING KUBECTL ACCESS ==="
if [ -f ./kubeconfig.yaml ]; then
    echo "✓ kubeconfig.yaml exists"
    echo "File size: $(stat -c%s ./kubeconfig.yaml 2>/dev/null || stat -f%z ./kubeconfig.yaml 2>/dev/null || echo 'unknown') bytes"
else
    echo "✗ kubeconfig.yaml NOT found"
fi

echo "Testing kubectl connection..."
kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify cluster-info --request-timeout=10s 2>&1 | head -3

echo "Listing all pods in namespace $NAMESPACE..."
kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pods -n "$NAMESPACE" --request-timeout=10s 2>&1 | head -10

echo "=== END DEBUG ==="
echo ""

# Function to copy file with retry logic
copy_file_with_retry() {
    local file="$1"
    local pod_name="$2"
    local namespace="$3"
    local max_retries="$4"
    local filename=$(basename "$file")
    
    for ((i=1; i<=max_retries; i++)); do
        echo "Copying $filename (attempt $i/$max_retries)..."
        
        # Use timeout command to limit kubectl execution time
        # Try copying to "worker" container first, then try containers with "worker" in name
        success=false
        
        # Method 1: Try exact "worker" container name
        if timeout $KUBECTL_TIMEOUT kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify cp "$file" "$namespace/$pod_name:/home/masa/" -c worker --request-timeout=${KUBECTL_TIMEOUT}s 2>/dev/null; then
            success=true
        else
            # Method 2: Try to find container with "worker" in name
            worker_containers=$(kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pod "$pod_name" -n "$namespace" -o jsonpath='{.spec.containers[?(@.name contains "worker")].name}' 2>/dev/null)
            
            if [ -n "$worker_containers" ]; then
                for container in $worker_containers; do
                    if timeout $KUBECTL_TIMEOUT kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify cp "$file" "$namespace/$pod_name:/home/masa/" -c "$container" --request-timeout=${KUBECTL_TIMEOUT}s 2>/dev/null; then
                        success=true
                        break
                    fi
                done
            fi
        fi
        
        if [ "$success" = true ]; then
            echo "✓ Successfully copied $filename"
            return 0
        else
            local exit_code=$?
            if [ $exit_code -eq 124 ]; then
                echo "⚠ Timeout occurred while copying $filename (attempt $i/$max_retries)"
            else
                echo "⚠ Failed to copy $filename (attempt $i/$max_retries, exit code: $exit_code)"
            fi
            
            if [ $i -lt $max_retries ]; then
                echo "Waiting ${RETRY_DELAY}s before retry..."
                sleep $RETRY_DELAY
            fi
        fi
    done
    
    echo "✗ Failed to copy $filename after $max_retries attempts"
    return 1
}

# Convert comma-separated list to array
IFS=',' read -ra DEPLOYMENT_ARRAY <<< "$DEPLOYMENTS"

# Track overall success
OVERALL_SUCCESS=true
FAILED_FILES=()

# Process each deployment
for deployment in "${DEPLOYMENT_ARRAY[@]}"; do
    echo ""
    echo "Processing deployment: $deployment"
    
    # Try multiple methods to find the pod
    POD_NAME=""
    
    # Method 1: Try to find pod by app label matching deployment name
    POD_NAME=$(kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pods -n "$NAMESPACE" -l app="$deployment" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    
    # Method 2: If not found, try to find pod by name containing the deployment name
    if [ -z "$POD_NAME" ]; then
        POD_NAME=$(kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pods -n "$NAMESPACE" -o jsonpath='{.items[?(@.metadata.name contains "'$deployment'")].metadata.name}' 2>/dev/null | head -1)
    fi
    
    # Method 3: If still not found, try to find any pod with "worker" in the name that matches part of deployment
    if [ -z "$POD_NAME" ]; then
        # Extract the worker identifier (e.g., "juno" from "tee-worker-juno")
        worker_id=$(echo "$deployment" | sed 's/.*worker-//')
        POD_NAME=$(kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pods -n "$NAMESPACE" -o jsonpath='{.items[?(@.metadata.name contains "worker")].metadata.name}' 2>/dev/null | grep "$worker_id" | head -1)
    fi
    
    # Method 4: If still not found, try to find any pod containing "worker"
    if [ -z "$POD_NAME" ]; then
        POD_NAME=$(kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pods -n "$NAMESPACE" -o jsonpath='{.items[?(@.metadata.name contains "worker")].metadata.name}' 2>/dev/null | head -1)
    fi
    
    if [ -z "$POD_NAME" ]; then
        echo "Warning: No pod found for deployment $deployment in namespace $NAMESPACE, skipping..."
        continue
    fi
    
    echo "Found pod: $POD_NAME in namespace: $NAMESPACE"
    
    # Show available containers for debugging
    echo "Available containers in pod:"
    kubectl --kubeconfig ./kubeconfig.yaml --insecure-skip-tls-verify get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || echo "Could not list containers"
    echo "Copying cookie files to /home/masa/..."
    
    # Copy all JSON cookie files to the worker container in parallel
    declare -a bg_pids=()
    declare -a file_list=()
    
    # Collect all files to process
    for file in ./cookies/*.json; do
        if [ -f "$file" ]; then
            file_list+=("$file")
        fi
    done
    
    if [ ${#file_list[@]} -eq 0 ]; then
        echo "No cookie files found to copy"
    else
        echo "Starting parallel copy of ${#file_list[@]} files (max ${MAX_PARALLEL_COPIES} concurrent)..."
        
        # Process files in batches to limit concurrent operations
        for ((i=0; i<${#file_list[@]}; i+=MAX_PARALLEL_COPIES)); do
            # Start a batch of parallel processes
            batch_pids=()
            for ((j=i; j<i+MAX_PARALLEL_COPIES && j<${#file_list[@]}; j++)); do
                file="${file_list[$j]}"
                (
                    if ! copy_file_with_retry "$file" "$POD_NAME" "$NAMESPACE" "$MAX_RETRIES"; then
                        echo "FAILED:$(basename "$file")" > "/tmp/failed_$(basename "$file")_$$"
                    fi
                ) &
                batch_pids+=($!)
            done
            
            # Wait for this batch to complete
            for pid in "${batch_pids[@]}"; do
                wait "$pid"
            done
        done
        
        # Check for failures
        for file in "${file_list[@]}"; do
            if [ -f "/tmp/failed_$(basename "$file")_$$" ]; then
                OVERALL_SUCCESS=false
                FAILED_FILES+=("$(basename "$file") -> $deployment")
                rm -f "/tmp/failed_$(basename "$file")_$$"
            fi
        done
    fi
    
    echo "Cookie files processing completed for $deployment!"
done

echo ""
echo "All deployments processed!"

# Report final status
if [ "$OVERALL_SUCCESS" = true ]; then
    echo "✓ All cookie files copied successfully!"
    exit 0
else
    echo "⚠ Some files failed to copy:"
    for failed in "${FAILED_FILES[@]}"; do
        echo "  - $failed"
    done
    echo "Consider increasing KUBECTL_TIMEOUT or MAX_RETRIES environment variables"
    exit 1
fi