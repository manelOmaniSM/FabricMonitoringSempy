#!/usr/bin/env python
# coding: utf-8

# ## 01_SemanticLink_CapacityMetrics
# 
# New notebook

# In[ ]:


# Import necessary libraries from sempy_labs
import sempy_labs as labs
from sempy_labs import lakehouse as lake
from sempy_labs import directlake
import sempy_labs.report as rep
import sempy.fabric as fabric

# Import standard libraries for datetime handling
from datetime import datetime, timezone, timedelta

# Import pandas for DataFrame handling
import pandas as pd

import json, requests
import time

# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %load_ext sempy


# In[5]:


from sempy_labs import admin, graph

key_vault_uri = '' # Enter your key vault URI
key_vault_tenant_id = 'TenantID' # Enter the key vault key to the secret storing your Tenant ID
key_vault_client_id = 'ClientID' # Enter the key vault key to the secret storing your Client ID (Application ID)
key_vault_client_secret = 'ClientSecret' # Enter the key vault key to the secret storing your Client Secret


# ### Get the list of Fabric capacities and datasets

# In[ ]:


with labs.service_principal_authentication(
    key_vault_uri=key_vault_uri, 
    key_vault_tenant_id=key_vault_tenant_id,
    key_vault_client_id=key_vault_client_id,
    key_vault_client_secret=key_vault_client_secret):
    
    df_capacities = labs.admin.list_capacities()

df_capacities['run_timestamp'] = datetime.now()

labs.save_as_delta_table(
                    dataframe=df_capacities,
                    delta_table_name="list_capacities",
                    write_mode="overwrite",
                    merge_schema=True
                )


# In[ ]:


with labs.service_principal_authentication(
    key_vault_uri=key_vault_uri, 
    key_vault_tenant_id=key_vault_tenant_id,
    key_vault_client_id=key_vault_client_id,
    key_vault_client_secret=key_vault_client_secret):
    
    df_datasets = labs.admin.list_datasets()

df_datasets['run_timestamp'] = datetime.now()
df_datasets['Upstream Datasets'] = df_datasets['Upstream Datasets'].apply(lambda x: 'NA' if x == [] else x)
df_datasets['Users'] = df_datasets['Users'].apply(lambda x: 'NA' if x == [] else x)

labs.save_as_delta_table(
                    dataframe=df_datasets,
                    delta_table_name="list_datasets",
                    write_mode="overwrite",
                    merge_schema=True,
                )


# ### Get the list Top Consumer for each capacity

# ##### Step 1: Run SQL query and get the list of Capacity IDs

# In[8]:


capacity_df = spark.sql("""
    SELECT DISTINCT Capacity_Id 
    FROM list_capacities 
    WHERE Capacity_Id IN ('f5b2b389-eeb1-429d-9e6e-f9ddff2c9099')
""")
capacity_id_list = capacity_df.select('Capacity_Id').rdd.flatMap(lambda x: x).collect()


# ##### Step 2: Loop through Capacity IDs and run DAX queries

# In[ ]:


FabricMonitoringWorkspace= "Microsoft Fabric Capacity Metrics"
FabricMonitoringDataset="Fabric Capacity Metrics"

for capacity_id in capacity_id_list:
    dax_query = f"""
    DEFINE
        MPARAMETER 'CapacityID' = "{capacity_id}"

        VAR __DS0FilterTable = 
        TREATAS({{"{capacity_id}"}}, 'Capacities'[capacityId])

    EVALUATE
    SUMMARIZECOLUMNS(
        Capacities[capacityId],
        Capacities[Capacity Name],
        Items[WorkspaceId],
        Items[WorkspaceName],
        Items[ItemKind],
        Items[ItemId],
        Items[ItemName],
        Dates[Date],
        __DS0FilterTable,
        "CU", [Dynamic M1 CU Preview],
        "Duration", [Dynamic M1 Duration Preview],
        "Users", [Dynamic M1 Users Preview],
        "Memory", round([Dynamic M1 Memory Preview],2)
    )
    """
    
    # Run DAX queries
    print(f"Running DAX for Capacity ID: {capacity_id}")
    df_daxresult = fabric.evaluate_dax(FabricMonitoringDataset, dax_query, FabricMonitoringWorkspace)
    labs.generate_dax_query_view_url(dataset=FabricMonitoringDataset, workspace=FabricMonitoringWorkspace, dax_string=dax_query)


    # Rename columns to remove square brackets
    df_daxresult = df_daxresult.rename(columns={
        'Capacities[capacityId]': 'capacityId',
        'Capacities[Capacity Name]': 'capacityName',
        'Items[WorkspaceId]': 'workspaceId',
        'Items[WorkspaceName]': 'workspaceName',
        'Items[ItemKind]': 'itemKind',
        'Items[ItemId]': 'itemId',
        'Items[ItemName]': 'itemName',
        'Dates[Date]': 'date',
        '[CU]': 'CU',
        '[Duration]': 'duration',
        '[Users]': 'users',
        '[Memory]': 'memory'
    })

    # Add run timestamp
    df_daxresult['run_timestamp'] = datetime.now()
    df_daxresult['run_date'] = datetime.now().date()

    # Save Results to Delta table
    labs.save_as_delta_table(
        dataframe=df_daxresult,
        delta_table_name="capacity_metrics",
        write_mode="append",
        merge_schema=True,
    )


# ##### Step 3: Run SQL query and get the top 20 consumer of each capacity

# In[ ]:


df_topconsumer = spark.sql("""
WITH AggregatedMetrics AS (
    SELECT 
        capacityId,
        capacityName,
        workspaceId,
        workspaceName,
        itemKind,
        itemId,
        itemName,
        SUM(CU) AS totalCU,             
        SUM(duration) AS totalDuration,   
        SUM(users) AS totalUsers,        
        MAX(memory) AS maxMemory,      
        run_date  
    FROM capacity_metrics
    GROUP BY 
        capacityId,
        capacityName,
        workspaceId,
        workspaceName,
        itemKind,
        itemId,
        itemName,
        run_date
),
MaxRunDate AS (
    SELECT 
        MAX(run_date) AS max_runDate
    FROM capacity_metrics
),
RankedMetrics AS (
    SELECT 
        AM.capacityId,
        AM.capacityName,
        AM.workspaceId,
        AM.workspaceName,
        AM.itemKind,
        AM.itemId,
        AM.itemName,
        AM.totalCU,
        AM.totalDuration,
        AM.totalUsers,
        AM.maxMemory,
        AM.run_date,
        ROW_NUMBER() OVER (PARTITION BY AM.capacityId ORDER BY AM.totalCU DESC) AS row_num
    FROM AggregatedMetrics AM
    INNER JOIN MaxRunDate MR
        ON AM.run_date = MR.max_runDate
)
SELECT 
    capacityId,
    capacityName,
    workspaceId,
    workspaceName,
    itemKind,
    itemId,
    itemName,
    totalCU,
    totalDuration,
    totalUsers,
    maxMemory,
    run_date
FROM RankedMetrics
WHERE row_num <= 20
ORDER BY capacityId, totalCU DESC;
""")

# Convert to pandaframe

df_topconsumer_pandas = df_topconsumer.toPandas()
df_topconsumer_pandas['run_timestamp'] = datetime.now()

# Save result to delta table

labs.save_as_delta_table(
    dataframe=df_topconsumer_pandas,
    delta_table_name="capacity_topconsumer",
    write_mode="append",
    merge_schema=True,
)


# ### Vertipaq Analyzer and BPA on Top Consumer for each capacity

# In[11]:


Item_df = spark.sql("""
SELECT distinct capacityId,
			capacityName,
			workspaceName,
			itemName as datasetName ,
            dataset.Configured_By as owner,
			dataset.max_Created_Date as Created_Date
FROM capacity_topconsumer as capacity
INNER JOIN ( 
	select Workspace_Id, Dataset_Id,Configured_By, MAX(run_timestamp) AS max_Created_Date
	from list_datasets 
	GROUP BY Workspace_Id, Dataset_Id,Configured_By
	) as dataset 

ON UPPER(capacity.workspaceId) = UPPER(dataset.Workspace_Id)
WHERE itemKind="Dataset"
""")

Item_df_list = Item_df.select('workspaceName','datasetName', 'owner','Created_Date').distinct().collect()


# #### Run Vertipaq Analyzer

# In[ ]:


def process_vertipaq(workspace_Name, dataset_Name):
    try:
        
        # Run Vertipaq analysis
        print(f"Running Vertipaq for Workspace: {workspace_Name}, Dataset: {dataset_Name}")
        labs.vertipaq_analyzer(dataset=dataset_Name, workspace=workspace_Name, export='table')
        result_status = "Vertipaq scan complete."

    except Exception as e:
        # Log failure due to an error
        result_status = "Vertipaq scan NOT complete (error)."


# In[17]:


def process_vertipaq_withRefreshSkipping(workspace_Name, dataset_Name, max_creation_time):
    try:
        # Fetch dataset refresh history
        dataset_refresh_history_df = labs.get_semantic_model_refresh_history(dataset=dataset_Name, workspace=workspace_Name)
        
        # Ensure the 'End Time' column is parsed as datetime
        if 'End Time' in dataset_refresh_history_df.columns:
            dataset_refresh_history_df['End Time'] = pd.to_datetime(dataset_refresh_history_df['End Time'], errors='coerce')

        # Extract the maximum End Time
        if not dataset_refresh_history_df.empty and dataset_refresh_history_df['End Time'].notna().any():
            max_end_time = dataset_refresh_history_df['End Time'].max().replace(microsecond=0, tzinfo=None)
        else:
            print(
                f"Skipping: Dataset '{dataset_Name}' in Workspace '{workspace_Name}' - No Dataset refresh history available."
            )
            return  # Skip without making an entry 

        # Check conditions to decide processing or skipping
        if max_creation_time is not None:
            if max_end_time <= max_creation_time:
                print(
                    f"Skipping: Dataset '{dataset_Name}' in Workspace '{workspace_Name}' - "
                    f"Not refreshed after the last Dataset Deployment. "
                    f"(Dataset Creation Time: {max_creation_time}, Last Dataset Refresh Time: {max_end_time})"
                )
                return  # Skip without making an entry
        print(
            f"Processing: Dataset '{dataset_Name}' in Workspace '{workspace_Name}' - "
            f"(Max Creation Time: {max_creation_time if max_creation_time else 'Null'}, Last Refresh Time: {max_end_time})"
        ) 

        # Run Vertipaq analysis
        print(f"Running Vertipaq for Workspace: {workspace_Name}, Dataset: {dataset_Name}")
        labs.vertipaq_analyzer(dataset=dataset_Name, workspace=workspace_Name, export='table')
        result_status = "Vertipaq scan complete."

    except Exception as e:
        # Log failure due to an error
        result_status = "Vertipaq scan NOT complete (error)."


# In[ ]:


for row in Item_df_list:

    process_vertipaq(row.workspaceName, row.datasetName)
    time.sleep(5)  


# ##### Run BPA

# In[ ]:


for row in Item_df_list:
    labs.run_model_bpa(dataset=row.datasetName,\
                       workspace=row.workspaceName, \
                       extended=True, \
                       export=True)
                       #Setting extended=True will fetch Vertipaq Analyzer statistics and use them to run advanced BPA rules against your model

