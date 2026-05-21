# Fabric notebook source
# Bronze Layer: Ingest Work Items from Azure DevOps
# Extracts work items (all types) from all projects using WIQL + batch API
# Supports incremental loads via ChangedDate watermark

# METADATA
# {"default_lakehouse": "ado_bronze", "default_lakehouse_name": "ado_bronze", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
full_load = False
target_lakehouse = "ado_bronze"

# ---- Cell 1: Configuration ----
import requests
import json
import base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, col
from notebookutils import mssparkutils

# Enable high concurrency session sharing
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# Configuration
API_VERSION = "7.1"
WATERMARK_TABLE = f"{target_lakehouse}.ingestion_watermarks"
ENTITY_NAME = "work_items"
ADO_ORG_URL = "https://dev.azure.com/your-org"

# Get PAT from Key Vault via Fabric connection
pat = mssparkutils.credentials.getSecret("https://your-keyvault.vault.azure.net/", "ado-pat")

auth_header = {
    "Authorization": "Basic " + base64.b64encode(f":{pat}".encode()).decode(),
    "Content-Type": "application/json"
}

# ---- Cell 2: Watermark Management ----
def get_watermark(spark, entity_name):
    """Get the last ingestion watermark for incremental loads."""
    try:
        df = spark.sql(f"SELECT max_changed_date FROM {WATERMARK_TABLE} WHERE entity = '{entity_name}'")
        if df.count() > 0:
            return df.collect()[0]["max_changed_date"]
    except Exception:
        # Table doesn't exist yet - first run
        pass
    return None

def update_watermark(spark, entity_name, max_date):
    """Update the watermark after successful ingestion."""
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT '{entity_name}' AS entity, '{max_date}' AS max_changed_date) AS source
        ON target.entity = source.entity
        WHEN MATCHED THEN UPDATE SET max_changed_date = source.max_changed_date
        WHEN NOT MATCHED THEN INSERT (entity, max_changed_date) VALUES (source.entity, source.max_changed_date)
    """)

# ---- Cell 3: Azure DevOps API Helpers ----
def get_all_projects():
    """Fetch all projects in the organization."""
    url = f"{ADO_ORG_URL}/_apis/projects?api-version={API_VERSION}&$top=500"
    projects = []
    while url:
        resp = requests.get(url, headers=auth_header)
        resp.raise_for_status()
        data = resp.json()
        projects.extend(data["value"])
        url = data.get("continuationToken")
        if url:
            url = f"{ADO_ORG_URL}/_apis/projects?api-version={API_VERSION}&$top=500&continuationToken={url}"
    return projects

def query_work_item_ids(project_name, since_date=None):
    """Use WIQL to get work item IDs, optionally filtered by changed date."""
    wiql_url = f"{ADO_ORG_URL}/{project_name}/_apis/wit/wiql?api-version={API_VERSION}"
    
    where_clause = "WHERE [System.TeamProject] = @project"
    if since_date:
        where_clause += f" AND [System.ChangedDate] >= '{since_date}'"
    
    wiql = {
        "query": f"SELECT [System.Id] FROM WorkItems {where_clause} ORDER BY [System.ChangedDate] DESC"
    }
    
    resp = requests.post(wiql_url, headers=auth_header, json=wiql)
    resp.raise_for_status()
    return [item["id"] for item in resp.json().get("workItems", [])]

def get_work_items_batch(project_name, ids, batch_size=200):
    """Fetch work item details in batches of 200 (API limit)."""
    all_items = []
    
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        url = f"{ADO_ORG_URL}/{project_name}/_apis/wit/workitems?ids={','.join(map(str, batch_ids))}&$expand=all&api-version={API_VERSION}"
        resp = requests.get(url, headers=auth_header)
        resp.raise_for_status()
        all_items.extend(resp.json().get("value", []))
    
    return all_items

# ---- Cell 4: Main Ingestion Logic ----

# Determine watermark
watermark = None
if not full_load:
    watermark = get_watermark(spark, ENTITY_NAME)
    if watermark:
        print(f"Incremental load since: {watermark}")
    else:
        print("No watermark found - performing full load")

# Fetch all projects
projects = get_all_projects()
print(f"Found {len(projects)} projects")

# Collect work items from all projects
all_work_items = []
for project in projects:
    project_name = project["name"]
    print(f"Processing project: {project_name}")
    
    try:
        ids = query_work_item_ids(project_name, watermark)
        if ids:
            items = get_work_items_batch(project_name, ids)
            # Add project context
            for item in items:
                item["_project_name"] = project_name
                item["_project_id"] = project["id"]
            all_work_items.extend(items)
            print(f"  Found {len(items)} work items")
        else:
            print(f"  No work items to process")
    except Exception as e:
        print(f"  Error processing {project_name}: {e}")

print(f"\nTotal work items collected: {len(all_work_items)}")

# ---- Cell 5: Write to Bronze Delta Table ----
if all_work_items:
    # Convert to JSON strings for raw storage (Bronze = raw)
    raw_records = []
    for item in all_work_items:
        raw_records.append({
            "work_item_id": item.get("id"),
            "project_name": item.get("_project_name"),
            "project_id": item.get("_project_id"),
            "raw_json": json.dumps(item),
            "ingested_at": datetime.utcnow().isoformat()
        })
    
    df = spark.createDataFrame(raw_records)
    
    # Write/merge to Delta table
    table_name = f"{target_lakehouse}.bronze_work_items"
    
    if full_load or not spark.catalog.tableExists(table_name):
        df.write.mode("overwrite").format("delta").saveAsTable(table_name)
        print(f"Full load: wrote {df.count()} records to {table_name}")
    else:
        # Upsert using merge
        df.createOrReplaceTempView("new_work_items")
        spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING new_work_items AS source
            ON target.work_item_id = source.work_item_id AND target.project_id = source.project_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Incremental load: merged {df.count()} records into {table_name}")
    
    # Update watermark
    max_changed = max(
        item.get("fields", {}).get("System.ChangedDate", "1900-01-01")
        for item in all_work_items
        if "fields" in item
    )
    if max_changed != "1900-01-01":
        # Ensure watermark table exists
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
                entity STRING,
                max_changed_date STRING
            ) USING DELTA
        """)
        update_watermark(spark, ENTITY_NAME, max_changed)
        print(f"Updated watermark to: {max_changed}")
else:
    print("No work items to ingest")

print("Bronze work items ingestion complete!")
