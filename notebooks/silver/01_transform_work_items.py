# Fabric notebook source
# Silver Layer: Transform Work Items
# Flattens raw JSON from Bronze, normalizes fields, applies schema

# METADATA
# {"default_lakehouse": "ado_silver", "default_lakehouse_name": "ado_silver", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
bronze_lakehouse = "ado_bronze"
target_lakehouse = "ado_silver"

# ---- Cell 1: Setup ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp, lit, when,
    coalesce, element_at, split, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# Predefined schemafor ADO work item JSON (fields we extract)
wi_schema = StructType([
    StructField("id", IntegerType()),
    StructField("fields", StructType([
        StructField("System.Title", StringType()),
        StructField("System.WorkItemType", StringType()),
        StructField("System.State", StringType()),
        StructField("System.AssignedTo", StructType([
            StructField("displayName", StringType()),
            StructField("uniqueName", StringType())
        ])),
        StructField("System.IterationPath", StringType()),
        StructField("System.AreaPath", StringType()),
        StructField("System.CreatedDate", StringType()),
        StructField("System.ChangedDate", StringType()),
        StructField("System.Tags", StringType()),
        StructField("System.Description", StringType()),
        StructField("System.BoardColumn", StringType()),
        StructField("System.BoardColumnDone", BooleanType()),
        StructField("System.Parent", IntegerType()),
        StructField("System.Priority", IntegerType()),
        StructField("Microsoft.VSTS.Scheduling.StoryPoints", DoubleType()),
        StructField("Microsoft.VSTS.Common.BacklogPriority", DoubleType()),
        StructField("Microsoft.VSTS.Common.ClosedDate", StringType()),
        StructField("Microsoft.VSTS.Common.StateChangeDate", StringType()),
    ]))
])

# ---- Cell 2: Read Bronze Work Items ----
try:
    df_raw = spark.table(f"{bronze_lakehouse}.bronze_work_items")
    print(f"Bronze work items: {df_raw.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e).__name__):
        print(f"Skipping - bronze_work_items table not found: {e}")
        mssparkutils.notebook.exit("No bronze_work_items table found - skipping")
    raise

# ---- Cell 3: Flatten Work Items ----
df_parsed = df_raw.withColumn("parsed", from_json(col("raw_json"), wi_schema))

df_work_items = df_parsed.select(
    col("work_item_id").cast("int"),
    col("project_name"),
    col("project_id"),
    col("parsed.fields.`System.Title`").alias("title"),
    col("parsed.fields.`System.WorkItemType`").alias("work_item_type"),
    col("parsed.fields.`System.State`").alias("state"),
    col("parsed.fields.`System.AssignedTo`.displayName").alias("assigned_to"),
    col("parsed.fields.`System.AssignedTo`.uniqueName").alias("assigned_to_email"),
    col("parsed.fields.`System.IterationPath`").alias("iteration_path"),
    col("parsed.fields.`System.AreaPath`").alias("area_path"),
    to_timestamp(col("parsed.fields.`System.CreatedDate`")).alias("created_date"),
    to_timestamp(col("parsed.fields.`System.ChangedDate`")).alias("changed_date"),
    to_timestamp(col("parsed.fields.`Microsoft.VSTS.Common.ClosedDate`")).alias("closed_date"),
    to_timestamp(col("parsed.fields.`Microsoft.VSTS.Common.StateChangeDate`")).alias("state_change_date"),
    col("parsed.fields.`System.Tags`").alias("tags"),
    col("parsed.fields.`Microsoft.VSTS.Scheduling.StoryPoints`").alias("story_points"),
    col("parsed.fields.`System.Priority`").alias("priority"),
    col("parsed.fields.`System.Parent`").alias("parent_id"),
    col("parsed.fields.`System.BoardColumn`").alias("board_column"),
    col("parsed.fields.`System.BoardColumnDone`").alias("board_column_done"),
    col("parsed.fields.`Microsoft.VSTS.Common.BacklogPriority`").alias("backlog_priority"),
    col("parsed.fields.`System.Description`").alias("description"),
    col("ingested_at"),
    current_timestamp().alias("processed_at")
)

# Derive additional fields
df_work_items = df_work_items.withColumn(
    "iteration_name",
    element_at(split(col("iteration_path"), r"\\"), -1)
).withColumn(
    "is_closed",
    col("state").isin(["Closed", "Done", "Removed", "Resolved"])
).withColumn(
    "cycle_time_days",
    when(col("state").isin(["Closed", "Done", "Removed", "Resolved"]) & col("closed_date").isNotNull(),
         datediff(col("closed_date"), col("created_date")))
).withColumn(
    "age_days",
    when(~col("state").isin(["Closed", "Done", "Removed", "Resolved"]),
         datediff(current_timestamp(), col("created_date")))
)

# ---- Cell 4: Write Silver Work Items ----
df_work_items.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_work_items")
print(f"Silver work items: {df_work_items.count()} records written")

# ---- Cell 5: Create Work Item Hierarchy View ----
# Build parent-child relationships for Epics → Features → Stories → Tasks
df_hierarchy = df_work_items.select(
    col("work_item_id"),
    col("parent_id"),
    col("work_item_type"),
    col("title"),
    col("project_name"),
    col("state"),
    col("story_points")
)

df_hierarchy.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_work_item_hierarchy")
print("Silver work item hierarchy written")

print("\nSilver work items transformation complete!")
