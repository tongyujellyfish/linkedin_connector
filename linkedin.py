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


#below configurations are set in the configurations file
PROJECT = config.PROJECT_ID  
DATASET = config.DATASET_ID  
TABLE = config.DESTINATION_TABLE 
ACCOUNT_ID = config.ACCOUNT_ID
SERVICE_CREDENTIALS = config.SERVICE_CREDENTIALS
ORG_ID = config.ORG_ID
TABLE_ORGANIC = config.TABLE_ORGANIC
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
        secret = response.payload.data.decode("UTF-8")
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

# Function to get a list of sent campaigns
def get_campaigns_list(account_id, headers,yesterday_ts_start, yesterday_ts_end):
    try:
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

        print(f"\nGetting campaigns list for Account {account_id}")
        campaign_list = []
        stop_datafetch =  False
        while not stop_datafetch:
            response_campaigns = requests.get(endpoint + '?' + url, headers = headers)
            data = response_campaigns.json()
            print("response code:",response_campaigns.status_code)
            if response_campaigns.status_code == 200:

                if 'elements' in data.keys():
                    data = data['elements']
                    print("total campaigns fetched (including inactive campaigns):",len(data))  
                else:
                    print("No campaign fetched")
                    stop_datafetch =  True
                    break    
            else:
                print(response.json())                                 
            #condition - I : no data returned                                  
            if  len(data) != 0:
                #condition - II : between 1 to 1000 records                             
                if  len(data)/1000 < 1 :
                    for items in data :
                        campaign_start = items['runSchedule']['start']/1000
                        campaign_end = items['runSchedule']['end']/1000 if 'end' in items['runSchedule'].keys() else yesterday_ts_end 
                        if (campaign_start <= yesterday_ts_start <= campaign_end or campaign_start <= yesterday_ts_end <= campaign_end) and items["status"] != 'DRAFT':
                            campaign_dict = {}
                            campaign_dict['id'] = (items['id'])
                            campaign_dict['name'] = (items['name'])
                            campaign_list.append(campaign_dict)
                    stop_datafetch = True                            
                else: 
                    for items in data :
                        campaign_start = items['runSchedule']['start']/1000
                        campaign_end = items['runSchedule']['end']/1000 if 'end' in items['runSchedule'].keys() else yesterday_ts_end 
                        if (campaign_start <= yesterday_ts_start <= campaign_end or campaign_start <= yesterday_ts_end <= campaign_end) and items["status"] != 'DRAFT':   
                            campaign_dict = {}
                            campaign_dict['id'] = (items['id'])
                            campaign_dict['name'] = (items['name'])
                            campaign_list.append(campaign_dict)
                        param_skip += 1000                              
            else:
                stop_datafetch = True
                print('no active campaign returned')
        print("active campaigns fetched:",len(campaign_list))  
       
    except Exception as e:
            print("Failed to get campaign list:%s\n" % e)
    return campaign_list                                  

def get_linkedin_data(campaign,yesterday,headers):    
    fields = ['clicks','comments','impressions','leadGenerationMailInterestedClicks','likes','shares','videoCompletions','videoViews']
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
    try:
        url = urllib.parse.urlencode(data)
        response = requests.get(endpoint + '?' + url, headers = headers)
        response_campaigns = response.json()
        print(f"\nGetting campaigns metrics for campaign {campaign['id']}")
        print("response code:",response.status_code)
        if response.status_code == 200:
            if 'elements' in response_campaigns.keys():
                elements = response_campaigns['elements']
                datadict = {}    
                for field in fields:
                    datadict[field] = elements[0][field] if len(elements) > 0 and field in elements[0].keys() else 0
                datadict['date'] =  yesterday
                datadict['id'] = campaign['id']
                datadict['name'] =  campaign['name']
                print(f"Campaign metrics retrieved for campaign {campaign['name']}")
                return [datadict] 
            else:
                print(f"No campaign metrics retrieved for campaign {campaign['name']}")
        else:
            print(response.json())
    except Exception as e:
            print("Failed to get campaign metrics:%s\n" % e)
    return []
def get_organic_list(account_id,yesterday_ts_start, yesterday_ts_end,last_year,access_token ):
    headers2 = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'cache-control': 'no-cache',
        'X-Restli-Protocol-Version': '2.0.0'
    }
    
    try:
        param_select = 100
        param_skip = 0
        print(f"\nGetting organic posts list for Account {account_id}")
        post_list = []
        stop_datafetch =  False
        while not stop_datafetch:
            response_posts = requests.get(f"https://api.linkedin.com/v2/ugcPosts?q=authors&authors=List(urn%3Ali%3Aorganization%3A{account_id})&count={param_select}&start={param_skip}", headers = headers2)
            data = response_posts.json()
            print("response code:",response_posts.status_code)
            if response_posts.status_code == 200:
                if 'elements' in data.keys():
                    data = data['elements']
                    data_filtered = list(filter(lambda x: x['created']['time']>last_year *1000,data ))
                    if len(data_filtered) == 0:
                        print("total posts fetched:",len(post_list))
                        stop_datafetch =  True
                        break
                    else:
                        for element in data_filtered:
                            post_dict ={}
                            # # media can be empty []
                            media = element['specificContent']['com.linkedin.ugc.ShareContent']['media']
                            if len(media) > 0 and 'title' in media[0].keys() :
                                post_dict['title'] =  media[0]['title']['text']
                            else:
                                post_dict['title'] = element['specificContent']['com.linkedin.ugc.ShareContent']['shareCommentary']['text']
                            post_dict['id'] = element['id']
                            post_dict['post_type'] = 'shares'
                            post_dict['created_time'] =element['created']['time']
                            if 'ugcPost' in post_dict['id']:
                                post_dict['post_type'] = 'ugcPosts'
                            post_list.append(post_dict)
    
                        
                        if  len(data_filtered) < param_select:
                            print("total posts fetched:",len(post_list))
                            stop_datafetch =  True
                            break
                        else:
                            param_skip += param_select
                else:
                    print("No post fetched")
                    stop_datafetch =  True
                    break     
            else:
                print(response.json())                                 
    except Exception as e:
            print("Failed to get orgnaic post list:%s\n" % e)
    return post_list   


def get_organic_data(yesterday_ts_start,yesterday_ts_end,headers,org_id,post):
    try: 
        post_type = post['post_type']
        post_id = post['id']
        print(f"\nGetting organic data for ognazation {org_id}: {post_id}")
        endpoint = 'https://api.linkedin.com/v2/organizationalEntityShareStatistics'
        data = {
            'q':'organizationalEntity',
            'organizationalEntity':'urn:li:organization:' + org_id,
            'timeIntervals.timeRange.start': yesterday_ts_start*1000,
            'timeIntervals.timeRange.end':yesterday_ts_end*1000,
        }
        data[post_type] = post_id
        url = urllib.parse.urlencode(data)
        response = requests.get(endpoint + '?' + url, headers = headers)
        print("response code:",response.status_code)
        if response.status_code == 200:
            response = response.json()
            dataList = []
            if 'elements' in response.keys() and len(response['elements']) >0:
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
                    datadict["organizationId"] = org_id
                    datadict["postType"] = post_type
                    datadict["postID"] = post_id
                    datadict['title'] = post['title']
                    datadict['createdTime'] = post['created_time']
                    dataList.append(datadict)
                print(f"Organic metrics retrieved for ognazation {org_id} : {post_id}") 
                return dataList
            else:
                print(f"No organic metrics found for ognazation {org_id} : {post_id}")
        else:
            print(response.json())
    except Exception as e:
            print("Failed to get organic data:%s\n" % e)
 
def main():
    secret = get_secret()
    access_token = check_token(secret)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'cache-control': 'no-cache'
    }

    fmt = "%Y-%m-%d"
    timezonelist = ['Australia/Melbourne']
    for zone in timezonelist:
        tz = timezone(zone)
        now_time = datetime.now(tz)
        yesterday = now_time - timedelta(5)  # 1days back
        date = datetime.strftime(yesterday, fmt)
        yesterday_ts_start = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,0,0,0)).timestamp() # the timestamp of start of yesterday
        yesterday_ts_end = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,23,59,59)).timestamp()
        last_year = datetime(now_time.year-1,now_time.month,now_time.day,0,0,0).timestamp()

        print("Time in GMT now %s" %now_time)
        print("Appending data for  %s"%date)

        # Getting Paid campaign data
        new_campaign_list = get_campaigns_list(ACCOUNT_ID, headers,yesterday_ts_start, yesterday_ts_end)

        campaign_data = []
        if len(new_campaign_list) != 0:
            for campaign in new_campaign_list:
                campaign_data += get_linkedin_data(campaign,yesterday,headers) 
        df = pd.DataFrame.from_dict(campaign_data)

        # init BQ client
        client = bigquery.Client()
        job_config = bigquery.job.LoadJobConfig()
        table_ref = client.dataset(DATASET).table(TABLE)
        table_ref_organic = client.dataset(DATASET).table(TABLE_ORGANIC)

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
        
        # Getting organic posts list                               
        new_post_list = get_organic_list('2942816',yesterday_ts_start, yesterday_ts_end,last_year,access_token)

        # Getting organic data
        organic_data = []
        if len(new_post_list) != 0:
            for post in new_post_list:                
                post_data = get_organic_data(yesterday_ts_start,yesterday_ts_end,headers,ORG_ID,post)
                if post_data:
                    organic_data += post_data
        df_organic = pd.DataFrame.from_dict(organic_data)        

        # writing organic data to BigQuery table
        try:
            if not df_organic.empty:
                job_organic = client.load_table_from_dataframe(df_organic,table_ref_organic,job_config=job_config,project=PROJECT)
                job_organic.result()
                print("Organic records written to BQ table")
            else:
                print('No organic data returned, BQ not called')            
        except Exception as e:
            print("Failed to write organic data to table:%s\n" % e)


# # For local debugging
# if __name__ == '__main__':



main()