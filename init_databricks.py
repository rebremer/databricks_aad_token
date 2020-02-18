# Based on work from Alexandre Gattiker, 
# See https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/

import requests
from azure.common.credentials import get_azure_cli_credentials
import base64
from azure.cli.core import get_default_cli
import time

resource_group = "<<your databricks resource group>>"
databricks_workspace = "<<your databricks workspace>>"
dbricks_location = "<<Azure location>>"
notebook = "testnotebook.py"
notebookRemote = "/testnotebook"
dbricks_api = f"https://{dbricks_location}.azuredatabricks.net/api/2.0"

def create_databricks_workspace():

    # 0. Deploy Databricks workspace, see https://azure.microsoft.com/nl-nl/resources/templates/101-databricks-workspace/
    # Key commands are as follows, make sure that ARM does not prompt you in case it needs to run automated
    # az group create --name <resource-group-name> --location <resource-group-location> #use this command when you need to create a new resource group for your deployment
    # az group deployment create --resource-group <my-resource-group> --template-uri https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/101-databricks-workspace/azuredeploy.json
    # Run Azure CLI in python
    response = get_default_cli().invoke(['group', 'create', '-n', resource_group, '-l', dbricks_location])
    print(response)
    response = get_default_cli().invoke(['group', 'deployment', 'create', '-g', resource_group, '--template-file', 'arm/azuredeploy.json'])
    print(response)

def get_aad_token_dbr():

    # From Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
    credentials, subscription_id = get_azure_cli_credentials()

    # Get a token for the global Databricks application. This value is fixed and never changes.
    adbToken =  credentials.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d").token

    # Get a token for the Azure management API
    azToken = credentials.get_token("https://management.core.windows.net/").token
    dbricks_auth = {
        "Authorization": f"Bearer {adbToken}",
        "X-Databricks-Azure-SP-Management-Token": azToken,
        "X-Databricks-Azure-Workspace-Resource-Id": (
            f"/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Databricks"
            f"/workspaces/{databricks_workspace}"
        )
    }

    return dbricks_auth

def upload_notebook(dbricks_auth):
    # Upload notebook to Databricks
    print("Upload notebook to Databricks workspace")
    with open("modelling/" + notebook) as f:
        notebookContent = f.read()

    # Encode notebook to base64
    string = base64.b64encode(bytes(notebookContent, 'utf-8'))
    notebookContentb64 = string.decode('utf-8')
    #print(notebookContentb64)

    notebookName, ext = notebook.split(".")
    print(notebookName)

    response = requests.post(f"{dbricks_api}/workspace/import", 
        headers= dbricks_auth,
        json={
            "content": notebookContentb64,
            "path": "/" + notebookName,
            "language": "PYTHON",
            "overwrite": "true",
            "format": "SOURCE"
        })

    # TBD: Expecting synchroneous result. Only result back when data is completely copied
    if response.status_code != 200:
        print("Error copying notebook: %s: %s" % (response.json()["error_code"], response.json()["message"]))
        exit(1)
    else:
        print("Copy succesfull")

def run_notebook(dbricks_auth):
    # Based on https://github.com/rebremer/devopsai_databricks/tree/master/project/services

    response = requests.post(f"{dbricks_api}/jobs/create",
        headers= dbricks_auth,
        json={
            "name": "Run AzureDevopsNotebook Job",
            "new_cluster": {
                "spark_version": "4.0.x-scala2.11",
                "node_type_id": "Standard_D3_v2",
                "spark_env_vars": {
                    'PYSPARK_PYTHON': '/databricks/python3/bin/python3',
                },
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2
                }
            },
            "notebook_task": {
                "notebook_path": notebookRemote,
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

    # 0. Deploy new Databricks workspace
    create_databricks_workspace()
    # 1. Get bearer token to authenticate to DataBricks (without PAT token)
    dbricks_auth = get_aad_token_dbr()
    # 2. Upload notebook to databricks
    upload_notebook(dbricks_auth)
    # 3. Run notebook
    # See https://github.com/rebremer/devopsai_databricks/blob/master/project/services/20_buildModelDatabricks.py
    time.sleep(300) # take some time before AAD an
    run_notebook(dbricks_auth)