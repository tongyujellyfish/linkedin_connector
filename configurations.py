
'''
@configurations file 

'''
# Set Google Cloud project ID
PROJECT_ID = '<porject_id>'
# Insert the dataset ID
DATASET_ID = '<dataset>'
#Destination table where we want paid campaign data to be populated 
DESTINATION_TABLE = '<table1>'
#Destination table where we want organic data to be populated 
TABLE_ORGANIC='<table2>'
#Service account key file
SERVICE_CREDENTIALS = 'service_credentials.json'
#Linkedin Campaign manager ID
ACCOUNT_ID = '<campaign_account_id>'
#Linkedin organization ID
ORG_ID = '<linkedin_org_id>'
#Secret manager version url for client secrets
SECRET_MANAGER_PATH = '<secret_manager_path>'
#Slack webhook for token expiry alert
SLACK_HOOK = '<hook_url>'
#Instruction doc on how to renew linkedin token
DOC_URL = '<doc_url>'