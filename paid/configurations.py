'''
@configurations file 

'''
#Service account key file
SERVICE_CREDENTIALS = 'service_credentials.json'
#Secret manager version url for client secrets
SECRET_MANAGER_PATH = ''
#Slack webhook for token expiry alert
SLACK_HOOK = ''
#Instruction doc on how to renew linkedin token
DOC_URL = ''
#Configs passed by Cloud Scheduler body,used in local testing
TEST_CONFIG ={"ad_account_id":"client_ad_accountId", "client_project_id":"client_gcp_project_id","destination_table":"projectId.datasetId.tableId","dataset_loc":"dataset_loc" }

