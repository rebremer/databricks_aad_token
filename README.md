# Azure Databricks client script using Service Principals
Fully automated Azure Databricks client script in Python that does the following:

- Create Azure Databricks Workspace
- Add Service Principal (SPN) to Databricks Workspace with cluster creation rights (SPN does not need admin rights nor any rights on Databricks Workspace Control Plane)
- Copy notebook to SPN's private Workspace in Databricks and run notebook on new cluster
- Delete SPN from Databricks Workspace

Based on https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/ from Alexandre Gattiker