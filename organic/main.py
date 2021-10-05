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
from google.oauth2 import service_account


#below configurations are set in the configurations file
SERVICE_CREDENTIALS = config.SERVICE_CREDENTIALS
SECRET_MANAGER_PATH = config.SECRET_MANAGER_PATH
SLACK_HOOK = config.SLACK_HOOK

def get_secret(name=SECRET_MANAGER_PATH):
    try:
        # Create the Secret Manager client.
        credentials_sm = service_account.Credentials.from_service_account_file(SERVICE_CREDENTIALS)
        client_sm = secretmanager.SecretManagerServiceClient(credentials=credentials_sm)

        # Build the resource name of the secret version.
        name = name

        # Access the secret version.
        response = client_sm.access_secret_version(request={"name": name})
        secret = base64.decodebytes(response.payload.data).decode("UTF-8")
        print('client secret loaded')
        return json.loads(secret)
    except Exception as e:
        print("Failed to get client secret:%s\n" % e)

def send_slack_alert(slack_data,webhook_url=SLACK_HOOK):    
    response = requests.post(
        webhook_url, data=json.dumps({'text':slack_data}),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
    else:
        print(f'slack alert sent: {slack_data}')

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
            else:
                print('Refresh token expiring soon')
                # only send slack alert when paid function is called to avoid too many warnings
            time.sleep(5)
            return response_json['access_token']
    except Exception as e:
            print("Failed to check refresh token expiry:%s\n" % e)

def get_organic_list(account_id,last_year,access_token):
    """
    Function to get organic posts list
    maxiumn campaign per API request is 100
    this API endpoint uses different header X-Restli-Protocol-Version 2.0.0
    function returns list of posts created within a year to match linkedin UI
    return value is [] if no post fetched
    """     
    post_list = []    
    try:
        headers2 = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'cache-control': 'no-cache',
            'X-Restli-Protocol-Version': '2.0.0'
        }
        param_select = 100
        param_skip = 0
        print(f"\nGetting organic posts list for Account {account_id}")        
        stop_datafetch =  False
        while not stop_datafetch:
            response_posts = requests.get(f"https://api.linkedin.com/v2/ugcPosts?q=authors&authors=List(urn%3Ali%3Aorganization%3A{account_id})&count={param_select}&start={param_skip}", headers = headers2)
            data = response_posts.json()
            print("Response code:",response_posts.status_code)
            if response_posts.status_code == 200:
                if 'elements' in data:
                    data = data['elements']
                    # only return posts created within 12 months to match UI
                    data_filtered = list(filter(lambda x: x['created']['time']>last_year *1000,data))
                    if not data_filtered:
                        print(f"Finished fetching organic posts for Account {account_id}, total: {len(post_list)}")
                        stop_datafetch =  True
                        break
                    else:
                        for element in data_filtered:
                            post_dict ={}
                            # # media can be empty []
                            media = element['specificContent']['com.linkedin.ugc.ShareContent']['media']
                            # find post title or text if it has no title (matching UI)
                            if media and 'title' in media[0]:
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
                            print(f"Finished fetching organic posts for Account {account_id}, total: {len(post_list)}")
                            stop_datafetch = True
                            break
                        else:
                            param_skip += param_select
                else:
                    print("No post fetched for Account {account_id}")
                    stop_datafetch =  True
                    break     
            else:
                print(f"Failed to get organic posts for Account {account_id}, response code: {response_posts.status_code}: {response_posts.text}")
                stop_datafetch =  True
                break                                                 
    except Exception as e:
        print("Failed to get orgnaic post list: %s\n" % e)
    return post_list  


def get_organic_data(yesterday_ts_start,today_ts_start,headers,org_id,post,date):
    """
    Function to return organic metrics for selected post on selected day (GMT yesterday). 
    Function return list of a dict, or None if post has no data for the day (will be skipped in BQ upload)
    Linkedin has two types of posts, ugcPosts or shares. Request param changes based on post type.
    """   
    try:
        post_type = post['post_type']
        post_id = post['id']
        print(f"\nGetting organic data for ognazation {org_id}: {post_id}")
        endpoint = 'https://api.linkedin.com/v2/organizationalEntityShareStatistics'
        data = {
            'q':'organizationalEntity',
            'organizationalEntity':'urn:li:organization:' + org_id,
            'timeIntervals.timeRange.start': yesterday_ts_start*1000,
            'timeIntervals.timeRange.end':today_ts_start*1000,
            post_type: post_id
        }
        url = urllib.parse.urlencode(data)
        response = requests.get(endpoint + '?' + url, headers = headers)
        print("Response code:",response.status_code)
        if response.status_code == 200:
            response = response.json()
            dataList = []
            if 'elements' in response and response['elements']:
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
                    datadict['date'] = date
                    dataList.append(datadict)
                print(f"Organic metrics retrieved for ognazation {org_id} : {post_id}") 
                return dataList
            else:
                print(f"No organic metrics found for ognazation {org_id} : {post_id}")
        else:
            print(f"API response failed, response code: {response.status_code}: {response.text}")
    except Exception as e:
        print("Failed to get organic data, error: %s\n" % e)
    return
 
def main(request):
    #read the data passed from the scheduler
    request_json = request.get_json(force=True)
    try:
        secret = get_secret()
        access_token = check_token(secret)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'cache-control': 'no-cache'
        }

        account_id = request_json['account_id']
        client_project_id = request_json['client_project_id']
        destination_table = request_json['destination_table']
        destination_loc =request_json['dataset_loc']
        
        # linkedin API only supports UTC timestamps based data. Keeping timezone function here in case the API will add timezone support in the future.
        fmt = "%Y-%m-%d"
        timezonelist = ['Etc/Greenwich']
        for zone in timezonelist:
            tz = timezone(zone)
            now_time = datetime.now(tz)
            yesterday = now_time - timedelta(1)  # 1 days back
            date = datetime.strftime(yesterday, fmt)

            # the timestamp of start of yesterday
            yesterday_ts_start = tz.localize(datetime(yesterday.year,yesterday.month,yesterday.day,0,0,0)).timestamp() 

            # the timestamp of start of today. Organic metric API does not work on yesterday_ts_start to yesterday_ts_end, but works on yesterday_ts_start to today_ts_start
            today_ts_start = yesterday_ts_start + 86400
        
            # for filtering organic posts
            last_year = datetime(now_time.year-1,now_time.month,now_time.day,0,0,0).timestamp()

            print("\nTime in GMT now %s" %now_time)
            print("Appending data for GMT %s"%date)

            # init BQ client
            scopes=["https://www.googleapis.com/auth/bigquery"]
            credentials = service_account.Credentials.from_service_account_file(SERVICE_CREDENTIALS, scopes=scopes)
            client_bq = bigquery.Client(credentials=credentials,project=client_project_id,location=destination_loc)
            job_config = bigquery.LoadJobConfig()                    
            # Getting organic posts list                               
            new_post_list = get_organic_list(account_id,last_year,access_token)
            time.sleep(5)

            # Getting organic data
            organic_data = []
            if new_post_list:
                for post in new_post_list:                
                    post_data = get_organic_data(yesterday_ts_start,today_ts_start,headers,account_id,post,date)
                    if post_data:
                        organic_data += post_data
            df_organic = pd.DataFrame.from_dict(organic_data)

            # writing organic data to BigQuery table
            try:
                if not df_organic.empty:
                    job_organic = client_bq.load_table_from_dataframe(df_organic,destination_table,job_config=job_config,project=client_project_id)
                    job_organic.result()
                    print("Organic records written to BQ table")
                else:
                    print('No organic data returned, BQ not called')            
            except Exception as e:
                print("Failed to write organic data to table:%s\n" % e)
                send_slack_alert(f'Linkedin organic data failed to write to BQ table {destination_table} for campaign accout {account_id}')
    except Exception as e:
        print("Failed to write organic data to table:%s\n" % e)
        send_slack_alert(f'Linkedin organic function failed for {request_json}, error: {e}')
    return 'finished'

# # Local testing, using mock package to  
# if __name__ == '__main__': 
#     from unittest.mock import Mock
#     data=config.TEST_CONFIG
#     main(Mock(get_json=Mock(return_value=data)))

