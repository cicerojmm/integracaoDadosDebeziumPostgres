import os
import boto3
import json


def _get_config_from_s3_bucket(bucket_config: str, config_path: str):
    """Get a json config file from aws s3 bucket
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket_config, config_path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def _generate_steps(
    bucket_raw: str, bucket_staged: str, bucket_curated: str, bucket_config: str,
    glue_database: str, config_path: str, spark_job_script: str, jars: str, tables: list
):
    """Generate steps using data from config file

    Args:
        bucket_raw (str): debezium-kafka-stack-account-environment-kafka-raw
        bucket_staged (str): debezium-kafka-stack-account-environment-kafka-staged
        bucket_curated (str): debezium-kafka-stack-account-environment-kafka-curated
        bucket_config (str): debezium-kafka-stack-account-environment-kafka-config
        glue_database (str): deltalake_environment
        config_path (str): path of config files
        spark_job_script (str): script Deltalake to run 
        jars (str): jars needed to run

    Returns:
        list: list of steps to run
    """    
    key_path_cdc = 'kafka/cdc'
    key_path_processing = 'kafka/processing'
    key_path_archive = 'kafka/archive'


    #Move of raw to processing 
    steps_move_raw_processing_each_table = [{
        'Name': f'RAW-CopyToProcessing',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    's3-dist-cp',
                    '--src',
                    f's3://{bucket_raw}/{key_path_cdc}',
                    '--dest',
                    f's3://{bucket_raw}/{key_path_processing}',
                    '--deleteOnSuccess'
                ]
        },
    }]

    #Move of processing to archive 
    steps_move_processing_archive_each_table = [{
        'Name': f'RAW-CopyToArchive',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    's3-dist-cp',
                    '--src',
                    f's3://{bucket_raw}/{key_path_processing}',
                    '--dest',
                    f's3://{bucket_raw}/{key_path_archive}',
                    '--deleteOnSuccess'
                ]
        },
    }]

    #Process data in processing using DeltaLake script
    steps_deltalake_processing = [{
        'Name': f'RAW-STAGED-CURATED-{table["topic_name"]}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    "spark-submit",
                    "--conf",
                    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                    "--conf",
                    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "--class",
                    "deltaprocessing.Main",
                    "--jars",
                    jars,
                    spark_job_script,
                    json.dumps({**table, **{
                        "processing_prefix_path": f"s3://{bucket_raw}/{key_path_processing}",
                        "staged_prefix_path": f"s3://{bucket_staged}/{key_path_cdc}",
                        "curated_path": f"s3://{bucket_curated}/",
                        "glue_database": glue_database,
                    }}),
                ]
        },
    } for table in tables]

    steps_array = [
        *steps_move_raw_processing_each_table,
        *steps_deltalake_processing,
        *steps_move_processing_archive_each_table
    ]
    return steps_array


def handler(event, context):
    """Create a temporary cluster to process cdc files with deltalake
    """

    emr = boto3.client("emr")

    # Get Account ID
    accountID = boto3.client('sts').get_caller_identity().get('Account')

    # Get region name
    runtime_region = os.environ['AWS_REGION']

    environment = os.environ['ENV']
    keyName = os.environ['KEY_NAME']
    masterInstanceType = os.environ['MASTER_INSTANCE_TYPE']
    coreInstanceType = os.environ['CORE_INSTANCE_TYPE']
    ec2MasterName = os.environ['EC2_MASTER_NAME']
    ec2CoreName = os.environ['EC2_CORE_NAME']
    instanceCount = int(os.environ['INSTANCE_COUNT'])
    ebsSizeGB = int(os.environ['EBS_SIZE_GB'])
    ec2SubnetId = os.environ['EC2_SUBNET_ID']

    bucket_config = os.environ['BUCKET_CONFIG']
    jars = os.environ['JARS']

    config_path = os.environ['TABLES_CONF_PATH']

    bucket_name_raw = os.environ['BUCKET_NAME_RAW']
    bucket_name_staged = os.environ['BUCKET_NAME_STAGED']
    bucket_name_curated = os.environ['BUCKET_NAME_CURATED']

    glue_database = os.environ['GLUE_DATABASE']

    spark_job_script = os.environ['SPARK_JOB_SCRIPT']

    logUri = f"s3n://debezium-kafka-stack-{accountID}-{environment.lower()}-configs/logs/"
    releaseLabel = "emr-6.1.0"

    clusterName = f"EMR-DELTALAKE-{environment}-PROCESSING"

    tags = {
        'Owner': os.environ['Owner'],
        'PURPOSE': os.environ['PURPOSE'],
        'COST_CENTER': os.environ['COST_CENTER'],
        'Name': clusterName
    }

    config = _get_config_from_s3_bucket(
        bucket_config=bucket_config, config_path=config_path)

    tables=config

   

    steps = _generate_steps(
        bucket_raw=bucket_name_raw,
        bucket_staged=bucket_name_staged,
        bucket_curated=bucket_name_curated,
        bucket_config=bucket_config,
        glue_database=glue_database,
        config_path=config_path,
        spark_job_script=spark_job_script,
        jars=jars,
        tables=tables
    )

    response = emr.run_job_flow(
        Name=clusterName,
        LogUri=logUri,
        ReleaseLabel=releaseLabel,
        Tags=[
            {
                'Key': k,
                'Value': v
            }
            for k, v in tags.items()
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': ec2MasterName,
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': masterInstanceType,
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': ebsSizeGB
                                },
                                'VolumesPerInstance': 1
                            },
                        ]
                    },
                },
                {
                    'Name': ec2CoreName,
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': coreInstanceType,
                    'InstanceCount': instanceCount,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': ebsSizeGB
                                },
                                'VolumesPerInstance': 1
                            },
                        ]
                    }
                }
            ],
            'Ec2KeyName': keyName,
            'KeepJobFlowAliveWhenNoSteps': False,
            'Ec2SubnetId': ec2SubnetId,
            # To DEBUG
            # Allow the cluster to stay in Waiting after running the steps
            # 'KeepJobFlowAliveWhenNoSteps': True,
        },
        Steps=steps,
        Applications=[
            {
                'Name': 'Hadoop'
            },
            {
                'Name': 'Spark'
            },
        ],
        Configurations=[
            # Set heap memory of driver and executor automatic by instance size
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"}
            },
            # Add support to access AWS Glue catalog
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            # Feature from spark 3.0
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    "spark.sql.adaptative.enabled": "true"
                }
            },
            # Force EMR to use python 3
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        EbsRootVolumeSize=10,
        StepConcurrencyLevel=1
    )

    print(json.dumps(response, indent=4))

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
