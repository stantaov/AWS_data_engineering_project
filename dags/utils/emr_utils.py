
import boto3
import json
import requests
import time
import requests

# Retrives an instance region to use it later for creating boto3 clients
def get_region():
    instance_info = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    instance_info_dict = instance_info.json()
    return instance_info_dict.get('region') 

# Creates a boto3 ERM client 
def emr_clent(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)

# Creates an EMR cluster
def create_cluster(region_name, cluster_name='Spark-Cluster'):
    cluster = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel='emr-6.3.0',
        LogUri='s3://midterm-project-wcd/emr-logs/',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master Node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': "Worker Node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm3.xlarge',
                    'InstanceCount': 3
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'midterm',
            'EmrManagedMasterSecurityGroup': 'sg-0916a76e1f13d70c9',
            'EmrManagedSlaveSecurityGroup': 'sg-095446c5e365749e1',
        },
		# Adding HIVE and Spark metastores to regester them later with Glue Data Catalog
		Configurations=[{
			'Classification': 'hive-site',
			'Properties': {
				'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
			}
		}, 
			{
			'Classification': 'spark-hive-site',
			'Properties': {
				'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
			}
		}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            { 'Name': 'Hadoop'},
            { 'Name': 'Hive'},
            { 'Name': 'Spark'}
        ]
    )
    return cluster['JobFlowId']

# Gets the EMR current cluster status
def get_cluster_status(cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

# Terminates the EMR cluster
def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

# def get_cluster_name():
# 	response = emr.list_clusters(ClusterStates=['WAITING'])
# 	return response['Clusters'][0]['Id']

# Testing EMR cluster creation
# if __name__ == '__main__':
# 	region_name = get_region()
# 	emr_clent(region_name)
# 	create_cluster(region_name)