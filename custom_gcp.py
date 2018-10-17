import os, csv
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
import googleapiclient.discovery


#########################
#########################
##### CLOUD STORAGE #####
#########################
#########################
#https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/cloud-client/snippets.py


def list_buckets(project):
    storage_client = storage.Client(project=project)
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        print('Bucket {} found'.format(bucket.name))
    
def create_bucket(bucket_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.create_bucket(bucket_name)
    print('Bucket {} created'.format(bucket.name))
    
def delete_bucket(bucket_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()
    print('Bucket {} deleted'.format(bucket.name))
    
def list_blobs(bucket_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        print('File {} found'.format(blob.name))

def upload_blob(bucket_name, source_file_name, destination_blob_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}'.format(
        source_file_name,
        destination_blob_name))

def download_blob(bucket_name, source_blob_name, destination_file_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print('Blob {} downloaded to {}'.format(source_blob_name, destination_file_name))

def delete_blob(bucket_name, blob_name, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print('Blob {} deleted'.format(blob_name))
    
def delete_all_buckets_and_blobs(project):
    storage_client = storage.Client(project=project)
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()
            print('Blob {} deleted'.format(blob))
        bucket.delete()
        print('Bucket {} deleted'.format(bucket))
    
    
#########################
#########################
####### DATAPROC ########
#########################
#########################
#https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/dataproc/submit_job_to_cluster.py


def get_pyspark_file(filename):
    f = open(filename, 'rb')
    return f, os.path.basename(filename)

def get_region_from_zone(zone):
    try:
        region_as_list = zone.split('-')[:-1]
        return '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')

def upload_pyspark_file(project_id, bucket_name, filename, file):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_file(file)

def download_output(project_id, cluster_id, output_bucket, job_id):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(output_bucket)
    output_blob = (
        'google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'
        .format(cluster_id, job_id))
    return bucket.blob(output_blob).download_as_string()

def create_cluster(project, zone, region, cluster_name, 
                   master_type='n1-standard-1', 
                   worker_type='n1-standard-1', 
                   sec_worker_type='n1-standard-1',
                   no_masters=1, no_workers=2, no_sec_workers=1, 
                   sec_worker_preemptible=True, 
                   dataproc_version='1.2'):
    print('Creating cluster...')
    dataproc = get_client()
    zone_uri = \
        'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
            project, zone)
    # cluster_data defines cluster: https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig
    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'gceClusterConfig': {
                'zoneUri': zone_uri
            },
            'masterConfig': {
                'numInstances': no_masters,
                'machineTypeUri': master_type
            },
            'workerConfig': {
                'numInstances': no_workers,
                'machineTypeUri': worker_type
            },
            'secondaryWorkerConfig': {
                'numInstances': no_sec_workers,
                'machineTypeUri': sec_worker_type,
                "isPreemptible": sec_worker_preemptible
            },
            'softwareConfig': {
                'imageVersion': dataproc_version
            }
        }
    }
    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()
    return result

def wait_for_cluster_creation(project_id, region, cluster_name):
    print('Waiting for cluster creation...')
    dataproc = get_client()
    while True:
        result = dataproc.projects().regions().clusters().list(
            projectId=project_id,
            region=region).execute()
        cluster_list = result['clusters']
        cluster = [c
                   for c in cluster_list
                   if c['clusterName'] == cluster_name][0]
        if cluster['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        if cluster['status']['state'] == 'RUNNING':
            print("Cluster created.")
            break

def list_clusters_with_details(project, region):
    dataproc = get_client()
    result = dataproc.projects().regions().clusters().list(projectId=project, region=region).execute()
    if result:
        cluster_list = result['clusters']
        for cluster in cluster_list:
            print("{} - {}".format(cluster['clusterName'], cluster['status']['state']))
        return result
    else:
        print('There are no Dataproc Clusters in this Project and Region')

def get_cluster_id_by_name(cluster_list, cluster_name):
    cluster = [c for c in cluster_list if c['clusterName'] == cluster_name][0]
    return cluster['clusterUuid'], cluster['config']['configBucket']

def submit_pyspark_job(project, region, cluster_name, bucket_name, filename):
    dataproc = get_client()
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://{}/{}'.format(bucket_name, filename)
            }
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    return job_id

def delete_cluster(project, region, cluster):
    dataproc = get_client()
    print('Tearing down cluster')
    result = dataproc.projects().regions().clusters().delete(
        projectId=project,
        region=region,
        clusterName=cluster).execute()
    return result

def wait_for_job(project, region, job_id):
    dataproc = get_client()
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project,
            region=region,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished.')
            return result

def get_client():
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc


# Picks a PySpark file from a local file
# Creates a new Dataproc cluster
# Uploads the PySpark file to GCS
# Connects to created Dataproc cluster
# Runs job in Dataproc cluster
# Waits for job to complete, writes results to GCS bucket
# Downloads results from GCS to local file
# Deletes Dataproc cluster
def submit_pyspark_job_to_cluster(project_id, zone, cluster_name, 
                                  bucket_name, pyspark_file=None, create_new_cluster=True, 
                                  master_type='n1-standard-1', 
                                  worker_type='n1-standard-1', 
                                  sec_worker_type='n1-standard-1',
                                  no_masters=1,
                                  no_workers=2, 
                                  no_sec_workers=1, 
                                  sec_worker_preemptible=True, 
                                  dataproc_version='1.2'):
    region = get_region_from_zone(zone)
    try:
        spark_file, spark_filename = get_pyspark_file(pyspark_file)

        if create_new_cluster:
            create_cluster(project_id, zone, region, cluster_name)
            wait_for_cluster_creation(project_id, region, cluster_name)

        upload_pyspark_file(project_id, bucket_name, spark_filename, spark_file)
        cluster_list = list_clusters_with_details(project_id, region)['clusters']
        (cluster_id, output_bucket) = (get_cluster_id_by_name(cluster_list, cluster_name))

        job_id = submit_pyspark_job(project_id, region, cluster_name, bucket_name, spark_filename)
        wait_for_job(project_id, region, job_id)

        output = download_output(project_id, cluster_id, output_bucket, job_id)
        print('Received job output {}'.format(output))
        return output
    finally:
        if create_new_cluster:
            delete_cluster(project_id, region, cluster_name)
        spark_file.close()


#########################
#########################
####### BIGQUERY ########
#########################
#########################
# https://googleapis.github.io/google-cloud-python/latest/bigquery/index.html


def list_datasets(project):
    client = bigquery.Client(project=project)
    datasets = list(client.list_datasets())

    if datasets:
        print('Datasets in project {}:'.format(project))
        for dataset in datasets:
            print('\t{}'.format(dataset.dataset_id))
    else:
        print('{} project does not contain any datasets'.format(project))
        
def create_dataset(project, dataset_id):
    client = bigquery.Client(project=project)
    datasets = list(client.list_datasets())
    
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)
        print('Dataset {} created'.format(dataset_id))
    except:
        print('Dataset {} already exists'.format(dataset_id))
        
def delete_dataset(project, dataset_id): 
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)
        client.delete_dataset(dataset_ref, delete_contents=True)  
        print('Dataset {} deleted'.format(dataset_id))
    except:
        print('Dataset {} does not exist'.format(dataset_id))
        
def list_tables(project, dataset_id):
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref)) 

    if tables:
        print('Tables in dataset {}:'.format(dataset_id))
        for table in tables:
            print('\t{}'.format(table.table_id))
    else:
        print('{} dataset does not contain any tables'.format(dataset_id))
        
def create_table(project, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)

        schema = [
            bigquery.SchemaField('full_name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
        ]
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print('Table {} created'.format(table_id))
    except:
        print('Table {} already exists'.format(table_id))
        
def get_table(project, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)

        print(table.schema)
        print(table.description)
        print(table.num_rows)
    except:
        print('Table {} does not exist'.format(table_id))
        
def insert_in_table(project, table_id, dataset_id, rows):
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)
        errors = client.insert_rows(table, rows) 
    except:
        print('Table {} does not exist'.format(table_id))
        
def query_table(project, query):
    try:
        QUERY = (query)
        client = bigquery.Client(project=project)
        query_job = client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            print(row)
    except:
        print('Table {} does not exist'.format(table_id))
        
def delete_table(project, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project)
        table_ref = client.dataset(dataset_id).table(table_id)
        client.delete_table(table_ref)
        print('Table {}:{} deleted'.format(dataset_id, table_id))
    except:
        print('Table {}:{} does not exist'.format(dataset_id, table_id))
        
def extract_table_to_gcs(project, dataset_id, table_id, destination_uri):
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)

        extract_job = client.extract_table(table_ref, destination_uri, location='US')
        extract_job.result()  
        print('Exported {}:{}.{} to {}'.format(project, dataset_id, table_id, destination_uri))
    except:
        print('Could not export {}:{}.{} to {}'.format(project, dataset_id, table_id, destination_uri))
        
def load_gcs_parquet_to_table(project, table_id, dataset_id, uri):
    try:
        client = bigquery.Client(project=project)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        
        print('Starting job {}'.format(load_job.job_id))
        
        load_job.result()
        print('Job finished')

        destination_table = client.get_table(table_ref)
        print('Loaded {} rows'.format(destination_table.num_rows))
    except:
        print('Error')