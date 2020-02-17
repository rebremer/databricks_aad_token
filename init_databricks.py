# Based on work from Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/

import requests
from azure.common.credentials import get_azure_cli_credentials
import base64
from azure.mgmt.resource import ResourceManagementClient

resource_group = "<<your resource group>>"
databricks_workspace = "<<your databricks workspace>>"
dbricks_location = "westeurope"
notebook = "testnotebook.py"
dbricks_api = f"https://{dbricks_location}.azuredatabricks.net/api/2.0"

if __name__ == "__main__":

    # 0. Deploy Databricks workspace, see https://azure.microsoft.com/nl-nl/resources/templates/101-databricks-workspace/
    # Key commands are as follows, make sure that ARM does not prompt you in case it needs to run automated
    # az group create --name <resource-group-name> --location <resource-group-location> #use this command when you need to create a new resource group for your deployment
    # az group deployment create --resource-group <my-resource-group> --template-uri https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/101-databricks-workspace/azuredeploy.json

    # 1. Get bearer token to authenticate to DataBricks (without PAT token)
    dbricks_auth = get_aad_token_dbr()
    # 2. Upload notebook to databricks
    upload_notebook(dbricks_auth)
    # 3. Run notebook
    # See https://github.com/rebremer/devopsai_databricks/blob/master/project/services/20_buildModelDatabricks.py
    # Make sure that PAT tokes is changed by AAD token in example above

def get_aad_token_dbr():

    # From Alexandre Gattiker, see https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
    credentials, subscription_id = get_azure_cli_credentials()

    # Get a token for the global Databricks application. This value is fixed and never changes.
    adbToken = credentials.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d").token

    # Get a token for the Azure management API
    azToken = credentials.get_token("https://management.core.windows.net/").token
    dbricks_auth = {
        "Authorization": f"Bearer {adbToken}",
        "X-Databricks-Azure-SP-Management-Token": azToken,
        "X-Databricks-Azure-Workspace-Resource-Id": (
            f"/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Databricks"
            f"/workspaces/{databricks_workspace}")}

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

