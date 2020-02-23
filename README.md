# databricks_aad_token
Python script that does the following:

- Add SPN to Databricks Workspace with cluster creation rights (SPN does not need admin rights and no need to have contributor rights on Databricks workspace control plane either)
- Copy notebook to SPN private workspace in Databricks and run notebook on cluster
- Delete SPN from Databricks workspace

Based on https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
