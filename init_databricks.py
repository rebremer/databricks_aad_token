# Based on work from Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/

import requests
from azure.common.credentials import get_azure_cli_credentials
import base64
from azure.cli.core import get_default_cli
import time
import json

resource_group = "<<databricks resource group>>"
databricks_workspace = "<<databricks workspace name>>"
# This SPN has non admin rights on Databricks nor contributor rights on Databricks workspace content plane
client_id="<<service principal application id>>" 
client_secret="<<service principal secret>>"

#databricks_workspace_id ="/subscriptions/513a7987-b0d9-4106-a24d-4b3f49136ea8/resourceGroups/blog-devaisec-rg/providers/Microsoft.Databricks/workspaces/blog-devaisec-dbr2"
dbricks_location = "westeurope"
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


def get_admin_token(credentials, resource):

    token =  credentials.get_token(resource).token

    return token

def get_spn_token(tenant_id, resource):

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    params = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'resource': resource,
        'client_secret': client_secret
    }

    response = requests.post(url, headers=headers, data=params)
    if response.status_code != 200:
        print(response.content)
        return

    # todo, remove clumsiness, no time
    return json.loads(response.content.decode("utf-8"))['access_token']

def create_tmp_dbrpat(dbricks_auth):

    response = requests.post(f"{dbricks_api}/token/create",
        headers= dbricks_auth,
        json={"lifetime_seconds": 100, "comment": "this is a temp token"}
    )

    return response.json()["token_value"]

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
            "path": "/Users/" + client_id + "/" + notebookName,
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
                "notebook_path": "/Users/" + client_id + "/" + notebookRemote
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

def add_spn(dbr_tmp_pat):

    # Add SPN only works with PAT token
    response = requests.post(f"{dbricks_api}/preview/scim/v2/ServicePrincipals", 
        headers= {
            "Content-Type": "application/scim+json",
            "Authorization": "Bearer " + dbr_tmp_pat
        }, 
        json = {
            "schemas":[
                "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
            ],
            "applicationId":f"{client_id}",
            "displayName": f"test-sp-{client_id}",
            "entitlements": [{ "value":"allow-cluster-create" }]
        }
    )
    print(response.json())
    return (response.json()["id"])

def get_spns(dbricks_auth, spn_id):
  
    # Using Databricks PAT
    #response = requests.get(f"{dbricks_api}/preview/scim/v2/ServicePrincipals", 
    #    headers= {
    #        "Accept": "application/scim+json",
    #        "Authorization": "Bearer " + dbr_tmp_pat
    #    }
    #)
    if spn_id != "":
        url = f"{dbricks_api}/preview/scim/v2/ServicePrincipals/{spn_id}"
    else:
        url = f"{dbricks_api}/preview/scim/v2/ServicePrincipals"

    # This also works with AAD bearer
    response = requests.get(url, headers = dbricks_auth)
    print (json.dumps(response.json(), sort_keys=True, indent=4))
    return response.json()

def check_spn_exists(dbricks_auth):
    spn_list = get_spns(dbricks_auth, "")
    for resource in spn_list["Resources"]:
        if resource["applicationId"] == client_id:
            print(f"client_id {client_id} already exists")
            return resource["id"]
    return ""

def delete_spn(dbr_tmp_pat, spn_id):

    # Delete SPN only works with PAT token
    response = requests.delete(f"{dbricks_api}/preview/scim/v2/ServicePrincipals/{spn_id}", 
         headers= {
            "Accept": "application/scim+json",
            "Authorization": "Bearer " + dbr_tmp_pat,

        }
    )
    print (response.status_code)

if __name__ == "__main__":

    # 1. get admin credentials, subscription and tenant
    credentials, subscription_id, tenant_id = get_azure_cli_credentials(with_tenant=True)
    # 2.1 Deploy new Databricks workspace
    create_databricks_workspace()
    # 2.2 Wait 5 minutes, since it takes some time before Workspace is initialized
    time.sleep(300)
    # 3. Get  token to authenticate to DataBricks, user needs to be admin in Databricks
    admin_adb_token = get_admin_token(credentials, "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d")
    admin_az_token = get_admin_token(credentials,"https://management.core.windows.net/")
    dbricks_admin_auth = get_dbr_auth(admin_adb_token, admin_az_token)
    # 4. Create tmp dbr pat token to authenticate create/delete SPN in SCIM interface (works only with PAT) 
    dbr_tmp_pat = create_tmp_dbrpat(dbricks_admin_auth)
    # 3. Add spn to Databricks and provide rights to SPN to run manage clusters
    spn_dbr_id = check_spn_exists(dbricks_admin_auth)
    if spn_dbr_id == "":
        spn_dbr_id = add_spn(dbr_tmp_pat)
    get_spns(dbricks_admin_auth, "")
    # 4. Get spn tokens to authenticate, spn is not admin
    # Get a token for the global Databricks application. This value is fixed and never changes.
    spn_adb_token = get_spn_token(tenant_id, "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d")   
    spn_az_token = get_spn_token(tenant_id, "https://management.core.windows.net/")
    dbricks_spn_auth = get_dbr_auth(spn_adb_token, spn_az_token)
    # 6. Upload notebook using SPN auth to SPN workspace
    upload_notebook(dbricks_spn_auth)
    # 7. Run notebook using SPN auth, see also https://github.com/rebremer/devopsai_databricks/blob/master/project/services/20_buildModelDatabricks.py
    run_notebook(dbricks_spn_auth)
    # 8. Delete SPN using PAT
    delete_spn(dbr_tmp_pat, spn_dbr_id)
    get_spns(dbricks_admin_auth, spn_dbr_id)