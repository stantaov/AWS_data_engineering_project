
import logging 
import boto3, json, pprint, requests, textwrap, time, requests
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator


def get_region():
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    return response_json.get('region')

def client(region_name):
    return boto3.client('emr', region_name=region_name)

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

region = get_region()
emr = client(region)

# Checks if the EMR cluster is up and running
class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)
    # poke function will be called and checks that the clsuter status is WAITING
    def poke(self, context):
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            logging.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            logging.info(e)
            return False


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [ClusterCheckSensor]