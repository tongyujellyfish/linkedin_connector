import requests
import time
from io import StringIO
import os
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from google.cloud import bigquery, secretmanager
import json
import urllib
import configurations as config
import math 
import base64

#below configurations are set in the configurations file
SERVICE_CREDENTIALS = config.SERVICE_CREDENTIALS
SECRET_MANAGER_PATH = config.SECRET_MANAGER_PATH
SLACK_HOOK = config.SLACK_HOOK
DOC_URL = config.DOC_URL

#service account's authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_CREDENTIALS

def get_secret(name=SECRET_MANAGER_PATH):
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = name

        # Access the secret version.
        response = client.access_secret_version(request={"name": name})
        secret = base64.decodebytes(response.payload.data).decode("UTF-8")
        print('client secret loaded')
        return json.loads(secret)
    except Exception as e:
        print("Failed to get client secret:%s\n" % e)

def send_slack_alert(time,webhook_url=SLACK_HOOK,doc_link=DOC_URL):
    slack_data = {'text': f'linkedin refresh token is expiring in {math.floor(time/60/60/24)} days, please go to {doc_link} and follow instructions to renew'}
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
    else:
        print('slack alert sent')

def check_token(secret):
    # renew access_token by refresh_token, refresh_token expires in 365 days and can only be renewed with user login and auth through brower. Send slack alert if refresh token renew is required.
    try:
        # time for sending slack alert
        send_alert_time = 60 * 60 * 24 * 30
        endpoint = 'https://www.linkedin.com/oauth/v2/accessToken?grant_type=refresh_token'
        data = {
            'client_id': secret['client_id'],
            'client_secret': secret['client_secret'],
            'refresh_token': secret['refresh_token']
        }
        response = requests.post(endpoint, data=data)
        if response.status_code == 200:
            response_json = response.json()
            refresh_token_expires_in = response_json['refresh_token_expires_in']
            if  refresh_token_expires_in > send_alert_time:
                print('Linkedin refresh token is not expiring yet')
                # new linkedin access token takes a few seconds to active, adding timeout to avoid error {"serviceErrorCode":65601,"message":"The token used in the request has been revoked by the user","status":401} 
                time.sleep(5)
                return response_json['access_token']
            else:
                print('Refresh token expiring soon, sending slack alert')
                send_slack_alert(refresh_token_expires_in)
    except Exception as e:
        print("Failed to check refresh token expiry:%s\n" % e)


def get_campaigns_list(account_id, headers,yesterday_ts_start, today_ts_start):
    """
    Function to get paid campaigns list
    maxiumn campaign per API request is 1000
    function returns list of active campaigns of selected day (GMT yesterday)
    inactive campaigns and drafts are ignored
    return value is None if no active campaign fetched
    """
    try:
        param_select = 1000
        param_skip = 0
        endpoint = 'https://api.linkedin.com/v2/adCampaignsV2'
        print(f"\nGetting campaigns list for Account {account_id}")
        campaign_list = []
        stop_datafetch =  False
        while not stop_datafetch:
            request_data = {
                'q':'search',
                'start': param_skip,
                'count': param_select,
                'search.account.values[0]': 'urn:li:sponsoredAccount:' + account_id,
                'search.test':'false'
            }
            url = urllib.parse.urlencode(request_data)
            response_campaigns = requests.get(endpoint + '?' + url, headers = headers)
            response_data = response_campaigns.json()
            print("Response code:",response_campaigns.status_code)
            if response_campaigns.status_code == 200:
                if 'elements' in response_data and response_data['elements']:
                    data = response_data['elements']
                    for items in data :
                        campaign_start = items['runSchedule']['start']/1000
                        campaign_end = items['runSchedule']['end']/1000 if 'end' in items['runSchedule'] else today_ts_start 
                        if (campaign_start <= yesterday_ts_start <= campaign_end or campaign_start <= today_ts_start <= campaign_end) and items["status"] != 'DRAFT':
                            campaign_dict = {}
                            campaign_dict['id'] = (items['id'])
                            campaign_dict['name'] = (items['name'])
                            campaign_list.append(campaign_dict)
                    print(f'Checking campaigns list, total campaign(s) (including inactive) {len(data)}, active campaign(s) {len(campaign_list)}')                                                   
                    #condition I : between 1 to 1000 records                             
                    if  len(data)/param_select < 1 :
                        stop_datafetch = True
                        break                            
                    else:
                    #condition II : more than 1000 records   
                        param_skip += param_select                               
                else:
                    #condition III : no data returned
                    stop_datafetch =  True
                    break                                 
            else:
                print(f"API failed with error {response_campaigns.status_code}: {response_campaigns.text}")
                return    
        print("Finished fetching campaign list. Total active campaigns fetched:",len(campaign_list))  
        return campaign_list  
    except Exception as e:
            print("Failed to get campaign list, error: %s\n" % e)
    return                                   

def get_linkedin_data(campaign,yesterday,headers,date):
    """
    Function to return all metrics (>60) for selected campaign and selected day (GMT yesterday). 
    API allows 20 metrics per request.
    API fields are nullable fields in BQ schema file (id, name and date are not API fields)
    Function return list of a dict, or empty [] if campaign has no data for the day (will be skipped in BQ upload)
    """    
    #find all API fields
    schema_file = open("bq_paid_schema.json", "r")
    schema_dictionary = json.load(schema_file)
    fields = fields = list(map(lambda x: x['name'],filter(lambda x: x['mode']=='NULLABLE',schema_dictionary)))
    MAX_FIELDS = 20
    try:
        #fetch 20 metrics per API request
        rounds = math.ceil(len(fields)/MAX_FIELDS)
        datadict = {}
        print(f"\nGetting campaigns metrics for campaign {campaign['id']}")
        for i in range(rounds):
            start_index = i * MAX_FIELDS
            end_index = (i+1) * MAX_FIELDS
            sub_fields = fields[start_index: end_index]
            endpoint = 'https://api.linkedin.com/v2/adAnalyticsV2'
            data = {
                'q':'analytics',
                'dateRange.start.year': yesterday.year,
                'dateRange.start.month': yesterday.month,
                'dateRange.start.day': yesterday.day,
                'dateRange.end.year': yesterday.year,
                'dateRange.end.month': yesterday.month,
                'dateRange.end.day': yesterday.day,
                'timeGranularity': 'DAILY',
                'campaigns[0]':'urn:li:sponsoredCampaign:' + str(campaign['id']),
                'pivot':'CAMPAIGN',
                'fields': ','.join(sub_fields)
            }
            url = urllib.parse.urlencode(data)
            response = requests.get(endpoint + '?' + url, headers = headers)
            response_campaigns = response.json()
            # print("Response code:",response.status_code)
            if response.status_code == 200:
                if 'elements' in response_campaigns and response_campaigns['elements']:
                    elements = response_campaigns['elements']                                      
                    for sub_field in sub_fields:
                        # API returns empty [] if all metrics are 0
                        if sub_field in elements[0]:
                            datadict[sub_field] = elements[0][sub_field]
            else:
                print(f"Failed to get metrics for campaign {campaign['id']}, response code: {response.status_code}: {response.text}")
                return []
        if datadict:
            datadict['date'] =  date
            datadict['id'] = campaign['id']
            datadict['name'] =  campaign['name']
            print(f"Campaign metrics retrieved for campaign {campaign['id']}")
            return [datadict] 
        else:
            print(f"Campaign {campaign['id']} returns no data")                      
    except Exception as e:
            print("Failed to get campaign metrics, error: %s\n" % e)
    return []

 
def main(request):
    secret = get_secret()
    access_token = check_token(secret)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'cache-control': 'no-cache'
    }
    #read the data passed from the scheduler
    request_json = request.get_json(force=True)
    ACCOUNT_ID = request_json['linkedin_campaign_id']
    PROJECT = request_json['gcp_project_id']
    DATASET = request_json['bq_dataset']
    TABLE = request_json['bq_table_paid']
    
    # linkedin API only supports UTC timestamps based data. Keeping timezone function here in case the API will add timezone support in the future.
    fmt = "%Y-%m-%d"
    timezonelist = ['Etc/Greenwich']
    for zone in timezonelist:
        tz = timezone(zone)
        now_time = datetime.now(tz)
        yesterday = now_time - timedelta(44)  # 1 days back
        date = datetime.strftime(yesterday, fmt)

        # the timestamp of start of yesterday
        yesterday_ts_start = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,0,0,0)).timestamp() 

        # the timestamp of start of today. Organic metric API does not work on yesterday_ts_start to yesterday_ts_end, but works on yesterday_ts_start to today_ts_start
        today_ts_start = yesterday_ts_start + 86400
      

        print("Time in GMT now %s" %now_time)
        print("Appending data for GMT %s"%date)

        # Getting Paid campaign data
        new_campaign_list = get_campaigns_list(ACCOUNT_ID, headers,yesterday_ts_start, today_ts_start)
        time.sleep(5)

        campaign_data = []
        if new_campaign_list:
            for campaign in new_campaign_list:
                campaign_data += get_linkedin_data(campaign,yesterday,headers,date)
        df = pd.DataFrame.from_dict(campaign_data)

        # init BQ client
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        table_ref = client.dataset(DATASET).table(TABLE)
        
        # writing paid campaign data to BigQuery table
        try:
            if not df.empty:
                job = client.load_table_from_dataframe(df,table_ref,job_config=job_config,project=PROJECT)
                job.result()
                print("Campaign data written to BQ table")
            else:
                print('No campaign data returned, BQ not called')        
        except Exception as e:
            print("Failed to write campaign data to table:%s\n" % e)
    return 'finished'


# # Local testing, using mock package to pass data from cloud scheduler 
# if __name__ == '__main__': 
#     from unittest.mock import Mock
#     data=config.TEST_CONFIG
#     main(Mock(get_json=Mock(return_value=data)))

