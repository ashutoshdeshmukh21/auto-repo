import boto3
import datetime

def lambda_handler(event, context):
    current_date = datetime.datetime.now()

    last_month = current_date - datetime.timedelta(days=current_date.day)
    last_month_name = last_month.strftime('%B')
    last_month_numeric = last_month.month
    last_month_year = last_month.year

    table_name = f"waf_logs_{last_month_name.lower()}_{last_month_year}"
    database = "automated"
    output_s3 = f"s3://automated-athena-test/athena-queries-results/unnamed-results"

    client = boto3.client('athena', region_name='us-east-1')

    create_table_query = f"""
    CREATE EXTERNAL TABLE `{database}`.`{table_name}` (
      `timestamp` bigint,
      `formatversion` int,
      `webaclid` string,
      `terminatingruleid` string,
      `terminatingruletype` string,
      `action` string,
      `terminatingrulematchdetails` array <
                                        struct <
                                            conditiontype: string,
                                            sensitivitylevel: string,
                                            location: string,
                                            matcheddata: array < string >
                                              >
                                         >,
      `httpsourcename` string,
      `httpsourceid` string,
      `rulegrouplist` array <
                          struct <
                              rulegroupid: string,
                              terminatingrule: struct <
                                                  ruleid: string,
                                                  action: string,
                                                  rulematchdetails: array <
                                                                       struct <
                                                                           conditiontype: string,
                                                                           sensitivitylevel: string,
                                                                           location: string,
                                                                           matcheddata: array < string >
                                                                              >
                                                                        >
                                                    >,
                              nonterminatingmatchingrules: array <
                                                                  struct <
                                                                      ruleid: string,
                                                                      action: string,
                                                                      overriddenaction: string,
                                                                      rulematchdetails: array <
                                                                                           struct <
                                                                                               conditiontype: string,
                                                                                               sensitivitylevel: string,
                                                                                               location: string,
                                                                                               matcheddata: array < string >
                                                                                                  >
                                                                                           >
                                                                        >
                                                                 >,
                              excludedrules: string
                                >
                           >,
    `ratebasedrulelist` array <
                             struct <
                                 ratebasedruleid: string,
                                 limitkey: string,
                                 maxrateallowed: int
                                   >
                              >,
      `nonterminatingmatchingrules` array <
                                        struct <
                                            ruleid: string,
                                            action: string,
                                            rulematchdetails: array <
                                                                 struct <
                                                                     conditiontype: string,
                                                                     sensitivitylevel: string,
                                                                     location: string,
                                                                     matcheddata: array < string >
                                                                        >
                                                                 >,
                                            captcharesponse: struct <
                                                                responsecode: string,
                                                                solvetimestamp: string
                                                                 >
                                              >
                                         >,
      `requestheadersinserted` array <
                                    struct <
                                        name: string,
                                        value: string
                                          >
                                     >,
      `responsecodesent` string,
      `httprequest` struct <
                        clientip: string,
                        country: string,
                        headers: array <
                                    struct <
                                        name: string,
                                        value: string
                                          >
                                     >,
                        uri: string,
                        args: string,
                        httpversion: string,
                        httpmethod: string,
                        requestid: string
                          >,
      `labels` array <
                   struct <
                       name: string
                         >
                    >,
      `captcharesponse` struct <
                            responsecode: string,
                            solvetimestamp: string,
                            failureReason: string
                              >
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION 's3://automated-athena-test/athena-logs/'
    """
    
    #LOCATION 's3://automated-athena-test/athena-logs/{last_month.year}/{last_month.month}'
    
    response = client.start_query_execution(
        QueryString=create_table_query,
        ResultConfiguration={
            'OutputLocation': output_s3,
        }
    )

    execution_id = response['QueryExecutionId']
    return f"Query Execution ID: {execution_id}"
