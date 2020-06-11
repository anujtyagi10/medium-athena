import boto3
import io
import re
import time
import datetime

params = {
    'region': 'Fill the region',
    'database': 'crm_data_structure_by_crawler',
    'bucket': 'your bucket name',
    'path': 'athena-big-files/outputScript',
    'query': "SELECT distinct record.account__c, record.net_amount__c,record.Billing_City__c FROM crm_data_structure_by_crawler.crm_test_mediumtest c cross join UNNEST(c.orders) as re(record) where record.account__c = '1876' ;"
}

session = boto3.Session()

def athena_query(client, params):

    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_to_s3(session, params, max_execution = 50):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    filename='No path'
    while (max_execution > 0 and (state in ['RUNNING'] or state in ['QUEUED'])):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            print('state is ' + state)
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)

    return filename

# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()

# Query Athena and get the s3 filename as a result
earlier = datetime.datetime.now()
print('start time is', earlier)

s3_filename = athena_to_s3(session, params)
print(s3_filename);
now = datetime.datetime.now()
print('End time is',now)
diff = now - earlier
print("Time taken" , diff)

# Removes all files from the s3 folder you specified, so be careful
#cleanup(session, params)
