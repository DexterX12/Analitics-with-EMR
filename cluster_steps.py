import boto3

REGION = "us-east-1"
S3_BUCKET = "datasetproject3"
SCRIPT_1 = f"s3://{S3_BUCKET}/scripts/step1_clean.py"
SCRIPT_2 = f"s3://{S3_BUCKET}/scripts/step2_analytics.py"
INSTANCE_TYPE = "m5.xlarge"
INSTANCE_COUNT = 3

steps = [
    {
        "Name": "Data cleaning",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", SCRIPT_1]
        }
    },
    {
        "Name": "Descriptive analysis",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", SCRIPT_2]
        }
    }
]

def run_steps():
    emr = boto3.client("emr", region_name=REGION)

    active_clusters = emr.list_clusters(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
    )

    # Si hay un clúster activo, agregar steps
    if active_clusters["Clusters"]:
        cluster_id = active_clusters["Clusters"][0]["Id"]
        print(f"Clúster activo detectado: {cluster_id}")
        response = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
        print(f"Steps añadidos al clúster {cluster_id}: {response['StepIds']}")

    # Si no, crear uno nuevo con los steps
    else:
        print("No se encontró clúster activo. Creando uno nuevo...")
        response = emr.run_job_flow(
            Name='DataCluster',
            LogUri='s3://aws-logs-526595352865-us-east-1/elasticmapreduce',
            ReleaseLabel='emr-7.9.0',
            ServiceRole='arn:aws:iam::526595352865:role/EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            Instances={
                'Ec2SubnetIds': ['subnet-019fc7e85ac912c1c'],
                'Ec2KeyName': 'vockey',
                'EmrManagedMasterSecurityGroup': 'sg-04118f901003fb38c',
                'EmrManagedSlaveSecurityGroup': 'sg-0b4708aa5749b3945',
                'InstanceGroups': [ ... ],  # Usa los grupos definidos en la conversión
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False
            },
            Applications=[
                {'Name': name} for name in [
                    'Flink', 'HCatalog', 'Hadoop', 'Hive', 'Hue',
                    'JupyterEnterpriseGateway', 'JupyterHub', 'Livy', 'Spark', 'Tez', 'Zeppelin'
                ]
            ],
            Configurations=[
                {
                    'Classification': 'jupyter-s3-conf',
                    'Properties': {
                        's3.persistence.bucket': 'benitezbuckets3',
                        's3.persistence.enabled': 'true'
                    }
                },
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                    }
                }
            ],
            AutoScalingRole='arn:aws:iam::526595352865:role/EMR_AutoScaling_DefaultRole',
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            AutoTerminationPolicy={'IdleTimeout': 3600},
            VisibleToAllUsers=True
        )
        print(f"Nuevo clúster lanzado con ID: {response['JobFlowId']}")