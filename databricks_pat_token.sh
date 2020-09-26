dbrworkspace="<<your databricks workspace name>>"
rg="<<your resource group>>"
loc="westeurope"
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
api_response=$(curl -v -X POST https://$loc.azuredatabricks.net"/api/2.0/token/create \
  -H "Authorization: Bearer $token" \
  -H "X-Databricks-Azure-SP-Management-Token:$azToken" \
  -H "X-Databricks-Azure-Workspace-Resource-Id:$wsId" \
  -d '{ "lifetime_seconds": 360000, "comment": "this is an example token - Azure DevOPS" }')
DATABRICKS_TOKEN=$(jq .token_value -r <<< "$api_response")
echo $DATABRICKS_TOKEN