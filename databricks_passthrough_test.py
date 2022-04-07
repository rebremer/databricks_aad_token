# Based on work from Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/

import requests
from azure.identity import AzureCliCredential # pip install azure-identity
from azure.mgmt.compute import ComputeManagementClient # pip install azure-mgmt-compute
import time
import json

resource_group = "<<databricks resource group>>"
databricks_workspace = "<<databricks workspace name>>"
databricks_cluster_id = "<<Id of high concurrency cluster already created in Databricks (does not have to be started and running)>>"

dbricks_location = "westeurope"
subscription_id = "<<your subscription id>>"
notebookRemote = "/mount_hc_storage"
dbricks_api = f"https://<<your databricks id, not cluster id>>.azuredatabricks.net/api/2.0"
azure_ad_user = "<<your azure AD user. User needs to be logged in Azure CLI before script is run and user needs to have a workspace in Databricks>>"

notebookRemote = "/mount_hc_storage" # make sure notebook is uploaded to your workspace in Databricks

def get_aad_token(credentials, resource):

    token =  credentials.get_token(resource).token

    return token

def get_dbr_auth(adb_token, az_token):

    # From Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
    dbricks_auth = {
        "Authorization": f"Bearer {adb_token}",
        "X-Databricks-Azure-SP-Management-Token": az_token,
        "X-Databricks-Azure-Workspace-Resource-Id": (
            f"/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Databricks"
            f"/workspaces/{databricks_workspace}"
        )
    }

    return dbricks_auth

def run_notebook(dbricks_auth):
    # Based on https://github.com/rebremer/devopsai_databricks/tree/master/project/services

    response = requests.post(f"{dbricks_api}/jobs/create",
        headers= dbricks_auth,
        json={
            "name": "Run AzureDevopsNotebook Job",
            "existing_cluster_id": databricks_cluster_id,
            "notebook_task": {
                "notebook_path": "/Users/" + azure_ad_user + "/" + notebookRemote
            }
        }
    )

    if response.status_code != 200:
        print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
        exit(2)

    #
    # Step 3: Start job
    #
    databricks_job_id = response.json()['job_id']
    response = requests.post(f"{dbricks_api}/jobs/run-now",
        headers= dbricks_auth,
        json={
            "job_id": + databricks_job_id
        }
    )

    if response.status_code != 200:
        print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
        exit(3)

    print(response.json()['run_id'])

    #
    # Step 4: Wait until job is finished
    #
    databricks_run_id = response.json()['run_id']
    scriptRun = 1
    count = 0
    while scriptRun == 1:
        response = requests.get(
            f"{dbricks_api}/jobs/runs/get?run_id={databricks_run_id}",
            headers= dbricks_auth
        )

        state = response.json()['state']
        life_cycle_state = state['life_cycle_state']
        print(state)

        if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            result_state = state['result_state']
            if result_state == "SUCCESS":
                print("run ok")
                scriptRun = 0
            #exit(0)
            else:
                exit(4)
        elif count > 180:
            print("time out occurred after 30 minutes")
            exit(5)
        else:
            count += 1
            time.sleep(30) # wait 30 seconds before next status update


if __name__ == "__main__":

    # 0. Prerequisites
    # - Python:
    #   - Python packages in requirements.txt are installed
    # - Storage
    #   - Storage account is created with a file system. testcsv.txt shall be uploaded to storage account
    #   - Azure AD user shall have storage blob contributor rights on storage account
    # - Databricks
    #  - Azure AD user shall have a workspace in Databricks
    #  - Notebook mount_hc_storage shall be uploaded to Databricks workspace of user
    #  - Parameters in notebook shall be changed to storage account, filesystem
    #  - High concurrency cluster is created in Databricks, Id of cluster is put in databricks_cluster_id variable
    # - Azure CLI
    #  - Azure CLI is installed and user is logged in


    #https://docs.microsoft.com/de-de/python/api/azure-common/azure.common.credentials?view=azure-python
    # 1. Get credentials from azure cli session
    credentials = AzureCliCredential()

    # 2. Generate Azure AD helper tokens
    aad_dbrgeneral_token = get_aad_token(credentials, "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d")
    aad_az_token = get_aad_token(credentials,"https://management.core.windows.net/")

    # 3. Generate Azure AD databricks workspace token
    aad_dbrworkspace_token = get_dbr_auth(aad_dbrgeneral_token, aad_az_token)
    
    # 4. Run notebook using Azure AD workspace token, see also https://github.com/rebremer/devopsai_databricks/blob/master/project/services/20_buildModelDatabricks.py
    run_notebook(aad_dbrworkspace_token)