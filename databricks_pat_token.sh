dbrworkspace="<<your databricks workspace name>>"
rg="<<your resource group>>"
loc="westeurope"
akv="<<your azure key vault name>>"
az extension add --name databricks
sleep 1m
az databricks workspace create -l $loc -n $dbrworkspace -g $rg --sku premium
sleep 5m
tenantId=$(az account show --query tenantId -o tsv)
wsId=$(az resource show \
  --resource-type Microsoft.Databricks/workspaces \
  -g "$rg" \
  -n "$dbrworkspace" \
  --query id -o tsv)
token_response=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d)
token=$(jq .accessToken -r <<< "$token_response")
# Get a token for the Azure management API
token_response=$(az account get-access-token --resource https://management.core.windows.net/)
azToken=$(jq .accessToken -r <<< "$token_response")
# You can also generate a PAT token. Note the quota limit of 600 tokens.
api_response=$(curl -v -X POST https://$loc.azuredatabricks.net/api/2.0/token/create \
  -H "Authorization: Bearer $token" \
  -H "X-Databricks-Azure-SP-Management-Token:$azToken" \
  -H "X-Databricks-Azure-Workspace-Resource-Id:$wsId" \
  -d '{ "lifetime_seconds": 360000, "comment": "this is an example token - Azure DevOPS" }')
DATABRICKS_TOKEN=$(jq .token_value -r <<< "$api_response")
echo $DATABRICKS_TOKEN
# Create keyvault, set/get key from key vault
az keyvault create --name $akv --resource-group $rg --location $loc 
az keyvault secret set -n pattoken --vault-name $akv --value $DATABRICKS_TOKEN
$DBrKeyFromKV=$(az keyvault secret show -n pattoken)
# Create cluster option 1: Use PAT token to create Cluster
api_response=$(curl -v -X POST https://$loc.azuredatabricks.net/api/2.0/clusters/create \
  -H "Authorization: Bearer $DBrKeyFromKV" \
  -d '{"cluster_name": "clusterPAT","spark_version": "6.6.x-scala2.11","node_type_id": "Standard_D3_v2","autoscale" : {"min_workers": 1,"max_workers": 2}}')
echo $api_response
# Create cluster option 2: Use SPN bearer token to create Cluster
api_response=$(curl -v -X POST https://$loc.azuredatabricks.net/api/2.0/clusters/create \
  -H "Authorization: Bearer $token" \
  -H "X-Databricks-Azure-SP-Management-Token:$azToken" \
  -H "X-Databricks-Azure-Workspace-Resource-Id:$wsId" \
  -d '{"cluster_name": "clusterSPNBearer","spark_version": "6.6.x-scala2.11","node_type_id": "Standard_D3_v2","autoscale" : {"min_workers": 1,"max_workers": 2}}')
echo $api_response
