# databricks_aad_token
Fully automated Databricks python scripts that functionally does the following:

- Create Databricks Workspace
- Add Service Principal (SPN) to Databricks Workspace with cluster creation rights (SPN does not need admin rights nor does it need to have any rights on Databricks Workspace control plane)
- Copy notebook to SPN private workspace in Databricks and run notebook on cluster
- Delete SPN from Databricks workspace

Based on https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
