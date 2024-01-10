import boto3
from datetime import datetime, timedelta
import time

def execute_query_and_save(client, query, query_name, database, s3_output, s3_bucket):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output}
    )

    query_execution_id = response['QueryExecutionId']

    max_attempts = 30
    delay_seconds = 2
    for attempt in range(max_attempts):
        query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            break
        elif status in ['FAILED', 'CANCELLED']:
            raise Exception(f"Athena query execution failed with status: {status}")

        time.sleep(delay_seconds)

    query_results = client.get_query_results(QueryExecutionId=query_execution_id)
    column_names = [col['Label'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    csv_data = "\n".join([
        '"' + '","'.join(column_names) + '"', 
        *[
            '"' + '","'.join([col.get('VarCharValue', '') for col in row['Data']]) + '"'
            for row in query_results['ResultSet']['Rows'][1:]
        ]
    ]) 

    account_id = boto3.client('sts').get_caller_identity().get('Account')
    current_time = datetime.utcnow()
    current_date_str = current_time.strftime('%Y%m%d')  
    current_date = datetime.utcnow()
    last_month = current_date - timedelta(days=current_date.day)
    start_of_last_month = datetime(last_month.year, last_month.month, 1)

    last_month_name = start_of_last_month.strftime('%B').lower()
    last_month_numeric = last_month.month
    last_month_year = last_month.year

    csv_file_name = f"{account_id}-{query_name}-{current_date_str}.csv"

    s3 = boto3.client('s3')
    s3.put_object(Body=csv_data.encode('utf-8'), Bucket=s3_bucket, Key=f"athena-queries-results/{last_month_year}/{last_month_numeric}/{csv_file_name}")

    return query_execution_id

def lambda_handler(event, context):
    athena_region = 'us-east-1'
    database = 'automated'
    s3_bucket = 'automated-athena-test'
    
    current_date = datetime.utcnow()
    last_month = current_date - timedelta(days=current_date.day)
    start_of_last_month = datetime(last_month.year, last_month.month, 1)

    last_month_name = start_of_last_month.strftime('%B').lower()
    last_month_numeric = last_month.month
    last_month_year = last_month.year

    s3_output = f"s3://{s3_bucket}/athena-queries-results/unnamed-results"

    table_name = f"waf_logs_{last_month_name}_{last_month_year}"

    client = boto3.client('athena', region_name=athena_region)

    queries = [
        {
            'query': f"""
            SELECT
                httprequest.clientip AS Blocked_IP,
                httprequest.country AS Country,
                COUNT(*) AS Blocked_Count
            FROM {database}.{table_name}
            WHERE action = 'BLOCK'
            GROUP BY httprequest.clientip, httprequest.country
            ORDER BY Blocked_Count DESC
            LIMIT 10;
            """,
            'name': 'top-ips-blocked'
        },
        {
            'query': f"""
            SELECT
                terminatingruleid AS WAF_Rule_ID,
                COUNT(*) AS Blocked_Count
            FROM {database}.{table_name}
            GROUP BY terminatingruleid
            LIMIT 10;
            """,
            'name': 'top-rules-blocked'
        },
        {
            'query': f"""
            SELECT
                httprequest.headers[1].value AS User_Agent,
                COUNT(*) AS Blocked_Count
            FROM {database}.{table_name}
            WHERE action = 'BLOCK'
            GROUP BY httprequest.headers[1].value
            ORDER BY Blocked_Count DESC
            LIMIT 10;
            """,
            'name': 'top-user-agents'
        }
    ]

    query_execution_ids = []

    for query_data in queries:
        query_execution_id = execute_query_and_save(
            client, query_data['query'], query_data['name'], database, s3_output, s3_bucket
        )
        query_execution_ids.append({query_data['name']: query_execution_id})

    return {
        'query_execution_ids': query_execution_ids
    }
