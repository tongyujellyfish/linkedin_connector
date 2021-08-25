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
DESTINATION_TABLE = config.DESTINATION_TABLE
CREDENTIALS_PATH = config.SERVICE_CREDENTIALS

#below configurations are set in the configurations file
PROJECT = config.PROJECT_ID  
DATASET = config.DATASET_ID  
TABLE = config.DESTINATION_TABLE 
ACCOUNT_ID = config.ACCOUNT_ID
client_secrets_path = config.SERVICE_CREDENTIALS
ORG_ID ="2942816"
# ORG_ID ="2414183"
TABLE_ORGANIC='linkedin_organic'



def GetService(api_name, api_version, scope, client_secrets_path):
    """
    Get a service that communicates to a Google API.
    Args:
    api_name: string The name of the api to connect to.
    api_version: string The api version to connect to.
    scope: A list of strings representing the auth scopes to authorize for the
    connection.
    client_secrets_path: string A path to a valid client secrets file.
    Returns:
    A service that is connected to the specified API.
    """
    # Load up credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    client_secrets_path, scopes=scope)
    # Build the service object.
    service = build(api_name, api_version, credentials=credentials,cache_discovery=False)

    return service

def get_secret():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = 'projects/997258499645/secrets/linkedin_ads/versions/8'

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    secret = response.payload.data.decode("UTF-8")
    return json.loads(secret)


def send_slack_alert(time):
    webhook_url = 'https://hooks.slack.com/services/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'
    doc_link = 'https://docs.google.com/document/d/13lMMAjFEUlIDeGCYJQL88N1ekH8TkMRWJPBN9RbsEIQ/edit'

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

# Function to get a list of sent campaigns
def get_campaigns_list(access_token, account_id,now_time,date,headers,yesterday_ts_start,yesterday_ts_end):
    print("Time in GMT now %s" %now_time)
    print("Appending data for  %s"%date)
    param_select = 1000
    param_skip = 0
    request_data = {
        'q':'search',
        'start': param_skip,
        'count': param_select,
        'search.account.values[0]': 'urn:li:sponsoredAccount:' + account_id,
        'search.test':'false'
    }
    endpoint = 'https://api.linkedin.com/v2/adCampaignsV2'
    url = urllib.parse.urlencode(request_data)

    print("\n\nget campaigns called")
    campaign_list = []
    stop_datafetch =  False
    while not stop_datafetch:
        response_campaigns = requests.get(endpoint + '?' + url, headers = headers)
        data = response_campaigns.json()
        print("response code:",response_campaigns.status_code)
  
        if 'elements' in data.keys():
            data = data['elements']
            print("total campaigns fetched (including inactive campaigns):",len(data))  
        else:
            print("No campaign fetched")
            stop_datafetch =  True
            break    
                                                                     
        #condition - I : no data returned                                  
        if  len(data) != 0:
            #condition - II : between 1 to 1000 records returned                              
            if  len(data)/1000 < 1 :
                for items in data :
                    campaign_start = items['runSchedule']['start']/1000
                    campaign_end = items['runSchedule']['end']/1000 if 'end' in items['runSchedule'].keys() else yesterday_ts_end 
                    if campaign_start <= yesterday_ts_start <= campaign_end or campaign_start <= yesterday_ts_end <= campaign_end:
                        campaign_dict = {}
                        campaign_dict['id'] = (items['id'])
                        campaign_dict['name'] = (items['name'])
                        campaign_list.append(campaign_dict)
                stop_datafetch = True                            
            else: 
                  for items in data :
                    campaign_start = items['runSchedule']['start']/1000
                    campaign_end = items['runSchedule']['end']/1000 if 'end' in items['runSchedule'].keys() else yesterday_ts_end 
                    if campaign_start <= yesterday_ts_start <= campaign_end or campaign_start <= yesterday_ts_end <= campaign_end:   
                        campaign_dict = {}
                        campaign_dict['id'] = (items['id'])
                        campaign_dict['name'] = (items['name'])
                        campaign_list.append(campaign_dict)
                    param_skip += 1000                              
        else:
            stop_datafetch = True
            print('no active campaign returned')
    print("active campaigns fetched:",len(campaign_list))  
    return campaign_list                                  

def get_linkedin_data(access_token, campaign,fields,yesterday,headers):
    print("\n\ncampaign metrics called")
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
        'fields': ','.join(fields)
    }
    url = urllib.parse.urlencode(data)
    response_campaigns = requests.get(endpoint + '?' + url, headers = headers)
    elements = response_campaigns.json()['elements']

    datadict = {}    
    for field in fields:
        datadict[field] = elements[0][field] if len(elements) > 0 and field in elements[0].keys() else 0
    datadict['date'] =  yesterday
    datadict['id'] = campaign['id']
    datadict['name'] =  campaign['name']
    return [datadict]

def get_organic_data(access_token,yesterday_ts_start,yesterday_ts_end,headers):
    endpoint = 'https://api.linkedin.com/v2/organizationalEntityShareStatistics'
    data = {
        'q':'organizationalEntity',
        'organizationalEntity':'urn:li:organization:' + ORG_ID,
        'timeIntervals.timeRange.start': yesterday_ts_start*1000,
        'timeIntervals.timeRange.end':yesterday_ts_end*1000,
    }
    url = urllib.parse.urlencode(data)
    response = requests.get(endpoint + '?' + url, headers = headers).json()
    dataList = []
    if 'elements' in response.keys():
        response = response['elements']
        for item in response:
            datadict = {}
            datadict["clickCount"] = item["totalShareStatistics"]["clickCount"]
            datadict["engagement"] = item["totalShareStatistics"]["engagement"]
            datadict["likeCount"] = item["totalShareStatistics"]["likeCount"]
            datadict["commentCount"] = item["totalShareStatistics"]["commentCount"]
            datadict["shareCount"] = item["totalShareStatistics"]["shareCount"]
            datadict["impressionCount"] = item["totalShareStatistics"]["impressionCount"]
            datadict["startTime"] = item["timeRange"]["start"]
            datadict["endTime"] = item["timeRange"]["end"]
            datadict["organizationId"] =ORG_ID
            print(datadict)
            dataList.append(datadict)
        print(f"Organic metrics retrieved for ognazation {ORG_ID}") 
    else:
        print(f"No organic metrics found for ognazation {ORG_ID}")
    return dataList
 
def main():
    #service account's authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = client_secrets_path
    secret = get_secret()
    access_token = check_token(secret)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'cache-control': 'no-cache'
    }

    fields = ['clicks','comments','impressions','leadGenerationMailInterestedClicks','likes','shares','videoCompletions','videoViews']

    fmt = "%Y-%m-%d"
    timezonelist = ['Australia/Melbourne']
    for zone in timezonelist:
        tz = timezone(zone)
        now_time = datetime.now(tz)
        yesterday = now_time - timedelta(5)  # 1days back
        date = datetime.strftime(yesterday, fmt)
        yesterday_ts_start = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,0,0,0)).timestamp() # the timestamp of start of yesterday
        yesterday_ts_end = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,23,59,59)).timestamp()
        organic_data = get_organic_data(access_token,yesterday_ts_start,yesterday_ts_end,headers)
        df_organic = pd.DataFrame.from_dict(organic_data)

        new_campaign_list = get_campaigns_list(access_token, ACCOUNT_ID,now_time,date,headers,yesterday_ts_start,yesterday_ts_end)
        time.sleep(5)

        campaign_data = []
        for campaign in new_campaign_list:
            campaign_data += get_linkedin_data(access_token, campaign,fields,yesterday,headers)
        df = pd.DataFrame.from_dict(campaign_data)

        # init BQ client
        client = bigquery.Client()
        job_config = bigquery.job.LoadJobConfig()
        table_ref = client.dataset(DATASET).table(TABLE)
        table_ref_organic = client.dataset(DATASET).table(TABLE_ORGANIC)

        # writing the data to BigQuery table
        try:
            job = client.load_table_from_dataframe(df,table_ref,job_config=job_config,project=PROJECT)
            job.result()
            print("records written to BQ table")        
        except Exception as e:
            print("Failed to write data to table:%s\n" % e)
            # writing the data to BigQuery table
        try:
            job_organic = client.load_table_from_dataframe(df_organic,table_ref_organic,job_config=job_config,project=PROJECT)
            job.result()
            print("organic records written to BQ table")        
        except Exception as e:
            print("Failed to write organic data to table:%s\n" % e)


# # For local debugging
# if __name__ == '__main__':

main()

# TODO1: draft campaign exclusion
# TODO2: error handling


