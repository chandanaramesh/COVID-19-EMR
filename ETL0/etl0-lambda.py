import sys
import time

import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr")
    # chooses the first cluster which is Running or Waiting
    # possibly can also choose by name or already have the cluster id
    clusters = conn.list_clusters()
    # choose the correct cluster
    clusters = [c["Id"] for c in clusters["Clusters"] 
                if c['Name']=='githubETL']
    if not clusters:
        sys.stderr.write("No valid clusters\n")
        sys.stderr.exit()
    # take the first relevant cluster
    cluster_id = clusters[0]
    # code location on your emr master node
    CODE_DIR = "/home/hadoop/"

    # spark configuration example
    step_args = ["/usr/bin/spark-submit",
                 CODE_DIR + "download_git.py"]

    step = {"Name": "what_you_do-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args
            }
        }
    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return "Added step: %s"%(action)