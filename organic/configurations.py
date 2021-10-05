'''
@configurations file 

'''
#Service account key file
SERVICE_CREDENTIALS = 'service_credentials.json'
#Secret manager version url for client secrets
SECRET_MANAGER_PATH = ''
#Configs passed by Cloud Scheduler body,used in local testing
TEST_CONFIG ={"account_id":"client_linkedin_organisation_id", "client_project_id":"client_gcp_project_id","destination_table":"projectId.datasetId.tableId","dataset_loc":"dataset_loc" }
