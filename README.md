# **Linkedin Connector**

## **Introduction**

Linkedin Connector has two parts: Paid campaign metrics and organic metrics.

Paid campaign metrics are retrieved from Linkedin [Reporting and ROI API,](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/getting-started?tabs=http) to provide a holistic view of campaign performance. 

Organic metrics are provided by [Page Management API](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/community-management/organizations).The organic metrics will help to grow and engage the organisation's page audience.

Both APIs retrieve data on UTC daily basis.

## **BigQuery Schema (Metric fields):**

Paid: All available paid campaign metrics. Details see bq_paid_schema.json

Organic: All api returned organic metrics. Details see bq_organic_schema.json

## **Pre-requisites:**

**JFAU datacollection accounts:**

Linkedin account: https://www.linkedin.com/in/jellyfish-au-test-4a144b212/

Developer account https://www.linkedin.com/developers/apps/69127913/

**Linkedin Access:**

Paid Campaign - Viewer access to Linkedin Campaign manager account

Organic - Admin access to Linkedin page

**Developer account OAuth 2.0 scopes:**

Paid Campaign - r_ads and r_ads_reporting

Organic - w_organization_social, r_organization_social and rw_organization_admin

OAuth Token needs to be renewed manually once a year. Instructions see [Doc](https://docs.google.com/document/d/1DYI44MXeiRL38HN-hUgJm-pho3zX0AhaW0a-k_h30rw/edit#)

**Linkedin account IDs:**

Paid Campaign - Linkedin Campaign manager account ID

Organic - Linkedin Organisation ID (can be found on organisation admin page)

## **GCP Details**

The Cloud Functions will be hosted in JFAU’s GCP account. Metric data retrieved from APIs will be pushed to client’s BigQuery tables. In order to access the client's BigQuery tables, the following are needed. \
**GCP IDs:**
* GCP project ID
* BigQuery dataset ID
* BigQuery table IDs (paid and organic tables)

BigQuery table can be created using bq_paid_schema.json and bq_organic_schema.json files: 

*bq mk --table &lt;gcp_project>:&lt;bq_dataset_id>.&lt;paid_table_id> bq_paid_schema.json \
bq mk --table &lt;gcp_project>:&lt;bq_dataset_id>.&lt;organic_table_id> bq_organic_schema.json \*


**GCP Access:**

Give &lt;developer_name>@jellyfish.com owner access to the project.

If GCP project owner access is not possible, request BigQuery Data Editor and Job User permission to the following service account:

drd-linkedin-bq@linkedin-327306.iam.gserviceaccount.com

Note : we are well aware of the principle of least privilege but we ask the maximum permission to avoid going back and forth for each additional access; once the service has been set up, the permissions can be reduced.

## **Cloud Scheduler Configurations:**

Once all the above is ready. Create one new Cloud Scheduler each for paid and organic functions. 
Configurations:
 - Target type: HTTP 
 - URL: &lt;Cloud function trigger URL> 
 - HTTP method: POST 
 - Frequency: Once daily. Each function needs to be scheduled at a
   different time to avoid issues with access token revoking. 
 - HTTP headers: Content-Type:application/octet-stream, User-Agent:
   Google-Cloud-Scheduler 
 - Body: 
    Paid: {"ad_account_id":"client_ad_accountId", "client_project_id":"client_gcp_project_id","destination_table":"projectId.datasetId.tableId","dataset_loc":"dataset_loc" }
   Organic: {"account_id":"client_linkedin_organisation_id", "client_project_id":"client_gcp_project_id","destination_table":"projectId.datasetId.tableId","dataset_loc":"dataset_loc" }
 - Auth header: Add OIDC token 
 - Service account:   drd-linkedin-bq@linkedin-327306.iam.gserviceaccount.com
 - Audience: &lt;Cloud function trigger URL>

