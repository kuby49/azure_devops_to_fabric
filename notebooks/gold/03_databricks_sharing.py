# Fabric notebook source
# Databricks Sharing Setup
# Configures OneLake shortcuts for Databricks access to Gold layer tables
# Run this once to create shortcuts from a Databricks-accessible location

# METADATA
# {"default_lakehouse": "ado_gold", "default_lakehouse_name": "ado_gold", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# ---- Cell 1: Documentation ----
"""
Databricks Integration Options:
================================

Option 1: OneLake Shortcuts (Recommended - Zero Copy)
------------------------------------------------------
Databricks can access OneLake Delta tables directly via:
- ABFS path: abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-name>/Tables/<table-name>
- Unity Catalog external location pointing to OneLake

Gold lakehouse paths:
- abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/fact_work_items
- abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/fact_sprint_metrics
- abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/fact_pull_requests
- abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/dim_iteration
- abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/dim_project
- (etc.)

Option 2: Unity Catalog External Tables
-----------------------------------------
In Databricks, create an external location and external tables:

```sql
-- Step 1: Create storage credential (Azure AD passthrough or service principal)
CREATE STORAGE CREDENTIAL onelake_cred
WITH (AZURE_MANAGED_IDENTITY = '<managed-identity-id>');

-- Step 2: Create external location
CREATE EXTERNAL LOCATION onelake_ado_gold
URL 'abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables'
WITH (STORAGE CREDENTIAL onelake_cred);

-- Step 3: Create external tables
CREATE TABLE catalog.schema.fact_work_items
LOCATION 'abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/fact_work_items';
```

Option 3: Fabric Mirroring to Databricks (if available)
--------------------------------------------------------
Use Fabric's built-in mirroring feature to sync Delta tables to Databricks.
"""

# ---- Cell 2: Verify Gold tables exist ----
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=== Gold Layer Tables Available for Databricks ===\n")
tables = spark.catalog.listTables()
for t in tables:
    count = spark.table(t.name).count()
    print(f"  {t.name}: {count} rows")

print("\n=== OneLake ABFS Paths ===\n")
ws_id = "00000000-0000-0000-0000-000000000001"
lh_name = "ado_gold"
base_path = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_name}/Tables"

for t in tables:
    print(f"  {base_path}/{t.name}")
