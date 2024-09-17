#!/bin/bash

# Check the number of arguments
if [ "$#" -ne 4 ]; then
    echo "Wrong number of arguments"
    echo "Usage: ./run-streamsets-job-on-ephemeral-k8s-deployment.sh <deployment_to_clone_id> <new_deployment_name> <job_id> <engine_label>"
    echo "Usage Example: $./run-streamsets-job-on-ephemeral-k8s-deployment.sh 6895a7c5-2fad-465c-b132-d0a3adac6e47:8030c2e9-1a39-11ec-a5fe-97c8d4369386 ephemeral-1 1aee8d54-e2cb-4edd-9f6e-f7dc95f370be:8030c2e9-1a39-11ec-a5fe-97c8d4369386 label-1"
    exit 1
fi

# Set Control Hub credentials
source private/sdk-env.sh


# Launch the SDK script
python3 python/run_streamsets_job_on_ephemeral_k8s_deployment.py ${1} ${2} ${3} ${4}

