#!/usr/bin/env python

"""
blah blah

Prerequisites:
 - Python 3.9+

 - StreamSets DataOps Platform SDK for Python v6.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html

 - StreamSets Platform API Credentials for a user with Organization Administrator role

 - An active StreamSets Kubernetes Environment with an online Kubernetes Agent
"""

import datetime
import os
import sys
import time
from streamsets.sdk import ControlHub

# Max time to wait for an engine to come online after the deployment is active
max_engine_wait_time_seconds = 120

# Max time to wait for the Job to become ACTIVE
max_wait_seconds_for_job_to_become_active = 120

# Max time to wait for Job to complete
max_job_completion_wait_time_seconds = 60 * 60 # default to one hour

# How often to check for updated status from Control Hub
update_frequency_seconds = 10

# Check the number of command line args
if len(sys.argv) != 5:
    print('Error: Wrong number of arguments')
    print(
        'Usage: $ python3 ./run-streamsets-job-on-ephemeral-k8s-deployment.py <deployment_to_clone_id> <new_deployment_name> <job_id> <engine_label>')
    print('Usage Example: $ python3 ./run-streamsets-job-on-ephemeral-k8s-deployment.sh '
          '6895a7c5-2fad-465c-b132-d0a3adac6e47:8030c2e9-1a39-11ec-a5fe-97c8d4369386 '
          'ephemeral-1 '
          '1aee8d54-e2cb-4edd-9f6e-f7dc95f370be:8030c2e9-1a39-11ec-a5fe-97c8d4369386 '
          'label-1')
    sys.exit(1)

# Get the command line args
deployment_to_clone_id = sys.argv[1]
new_deployment_name = sys.argv[2]
job_id = sys.argv[3]
engine_label = sys.argv[4]


def print_message(message):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' ' + message)


print_message('----')
print_message('Run StreamSets Job on Ephemeral Kubernetes Deployment')
print_message('----')
print_message('Source Deployment\'s ID: ' + deployment_to_clone_id)
print_message('New Deployment\'s name: ' + new_deployment_name)
print_message('Job ID: ' + job_id)
print_message('Engine Label: ' + engine_label)
print_message('----')

# Get Control Hub API credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')

# Connect to Control Hub
print_message('Connecting to Control Hub')
sch = ControlHub(credential_id=cred_id, token=cred_token)

# Find the Job
job = None
for j in sch.jobs:
    if j.job_id == job_id:
        job = j
        print_message('Found Job')
        break
if job is None:
    print_message('Error: Job was not found')
    sys.exit(1)

# Find the source deployment to be cloned
deployment_to_be_cloned = None
for d in sch.deployments:
    if d.deployment_id == deployment_to_clone_id:
        deployment_to_be_cloned = d
        print_message('Found source Deployment')
        break
if deployment_to_be_cloned is None:
    print_message('Error: Source Deployment to be cloned was not found')
    sys.exit(1)

# Clone the deployment
print_message('Cloning Deployment')
deployment = sch.clone_deployment(deployment_to_be_cloned, name=new_deployment_name, engine_labels=[engine_label])

# Retrieve the new deployment from Control Hub
deployment = sch.deployments.get(deployment_id=deployment.deployment_id)

# Start the deployment
print_message('Starting Deployment')
sch.start_deployment(deployment)

# Make sure the deployment is Active
deployment = sch.deployments.get(deployment_id=deployment.deployment_id)
if deployment.state == 'ACTIVE':
    print_message('Deployment is ACTIVE!')
else:
    print_message('Error: Deployment is not in an ACTIVE state. Deployment state is {}'.format(deployment.state))
    print_message('Inspect the failed Deployment in the Control Hub UI to diagnose the issue.')
    sys.exit(1)

# Wait for an engine with the desired label to come online
engine_wait_time_seconds = 0
engine_is_online = False
while engine_wait_time_seconds < max_engine_wait_time_seconds:
    for engine in sch.data_collectors:
        if engine_label in engine.reported_labels and engine.responding:
            print_message('Engine is online!')
            engine_is_online = True
            break
    if engine_is_online:
        break
    print_message('Waiting for engine to come online...')
    time.sleep(update_frequency_seconds)
    engine_wait_time_seconds += update_frequency_seconds
if not engine_is_online:
    print_message('Engine is not online')
    print_message('Inspect the failed Deployment in the Control Hub UI to diagnose the issue.')
    sys.exit(1)


# Set the Job's engine label
print_message('Setting the Job\'s engine label')
job.data_collector_labels = [engine_label]

# Start the Job
print_message('----')
print_message('Starting the job')
sch.start_job(job)

# Wait for Job to transition to Active
job.refresh()
wait_seconds = 0
while job.status.status != 'ACTIVE':
    job.refresh()
    print_message('Waiting for Job to become ACTIVE...')

    if wait_seconds > max_wait_seconds_for_job_to_become_active:
        sys.exit('Error: Timeout waiting for Job to become ACTIVE')

    time.sleep(update_frequency_seconds)
    wait_seconds += update_frequency_seconds

print_message('Job status is ACTIVE!')

# Wait for Job to complete or to timeout
print_message('Waiting for Job to complete...')
job.refresh()
wait_seconds = 0
while job.status.status != 'INACTIVE':
    job.refresh()
    print_message('Waiting for Job to complete...')
    time.sleep(update_frequency_seconds)
    wait_seconds += update_frequency_seconds
    if wait_seconds > max_job_completion_wait_time_seconds:
        print_message('Error: Timeout waiting for Job to complete')
        break
if job.status.status == 'INACTIVE':
    print_message('Job completed successfully')
else:
    print_message('Error: Job did not complete successfully')

print_message('Job status is ' + job.status.status)
print_message('----')

# Delete deployment
print_message('Stopping engine and deleting Deployment')
sch.delete_deployment(deployment)

print_message('----')
print_message('Done')
