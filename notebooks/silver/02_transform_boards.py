# Fabric notebook source
# Silver Layer: Transform Boards Data (Sprints, Backlog, Board Columns)
# Normalizes Bronze boards data into structured Silver tables

# METADATA
# {"default_lakehouse": "ado_silver", "default_lakehouse_name": "ado_silver", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
bronze_lakehouse = "ado_bronze"
target_lakehouse = "ado_silver"

# ---- Cell 1: Setup ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    explode, explode_outer, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, ArrayType
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# Schemas for ADO boards JSON
team_schema = StructType([
    StructField("description", StringType()),
    StructField("url", StringType())
])

iteration_schema = StructType([
    StructField("attributes", StructType([
        StructField("startDate", StringType()),
        StructField("finishDate", StringType()),
        StructField("timeFrame", StringType())
    ]))
])

capacity_schema = StructType([
    StructField("teamMember", StructType([
        StructField("displayName", StringType()),
        StructField("uniqueName", StringType())
    ])),
    StructField("activities", ArrayType(StructType([
        StructField("capacityPerDay", DoubleType()),
        StructField("name", StringType())
    ]))),
    StructField("daysOff", StringType())
])

backlog_schema = StructType([
    StructField("rank", IntegerType()),
    StructField("type", StringType()),
    StructField("workItemTypes", StringType())
])

# ---- Cell 2: Transform Teams ----
try:
    df_teams_raw = spark.table(f"{bronze_lakehouse}.bronze_teams")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping - required bronze tables not found")
        mssparkutils.notebook.exit("Bronze tables not found - skipping")
    raise

df_teams = df_teams_raw.withColumn("parsed", from_json(col("raw_json"), team_schema)).select(
    col("team_id"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    col("parsed.description").alias("description"),
    col("parsed.url").alias("url"),
    current_timestamp().alias("processed_at")
)

df_teams.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_teams")
print(f"Silver teams: {df_teams.count()} records")

# ---- Cell 3: Transform Iterations (Sprints) ----
df_iter_raw = spark.table(f"{bronze_lakehouse}.bronze_iterations")

df_iterations = df_iter_raw.withColumn("parsed", from_json(col("raw_json"), iteration_schema)).select(
    col("iteration_id"),
    col("iteration_name"),
    col("iteration_path"),
    col("team_name"),
    col("team_id"),
    col("project_name"),
    col("project_id"),
    to_timestamp(col("parsed.attributes.startDate")).alias("start_date"),
    to_timestamp(col("parsed.attributes.finishDate")).alias("finish_date"),
    col("parsed.attributes.timeFrame").alias("time_frame"),
    current_timestamp().alias("processed_at")
)

df_iterations.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_iterations")
print(f"Silver iterations: {df_iterations.count()} records")

# ---- Cell 4: Transform Sprint Capacity ----
try:
    df_cap_raw = spark.table(f"{bronze_lakehouse}.bronze_sprint_capacity")

    df_capacity = df_cap_raw.withColumn("parsed", from_json(col("raw_json"), capacity_schema)).select(
        col("iteration_id"),
        col("team_name"),
        col("project_name"),
        col("project_id"),
        col("parsed.teamMember.displayName").alias("member_name"),
        col("parsed.teamMember.uniqueName").alias("member_email"),
        col("parsed.activities")[0]["capacityPerDay"].cast("double").alias("capacity_per_day"),
        col("parsed.activities")[0]["name"].alias("activity"),
        col("parsed.daysOff").alias("days_off_json"),
        current_timestamp().alias("processed_at")
    )

    df_capacity.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_sprint_capacity")
    print(f"Silver sprint capacity: {df_capacity.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping sprint capacity - bronze table not found")
    else:
        raise

# ---- Cell 5: Transform Sprint Work Items ----
df_swi_raw = spark.table(f"{bronze_lakehouse}.bronze_sprint_work_items")

# Each row has a JSON array of work item relations for the sprint
df_sprint_items = df_swi_raw.select(
    col("iteration_id"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    explode(from_json(col("raw_json"), ArrayType(
        StructType([
            StructField("rel", StringType()),
            StructField("source", StructType([StructField("id", IntegerType())])),
            StructField("target", StructType([StructField("id", IntegerType())]))
        ])
    ))).alias("item")
).select(
    col("iteration_id"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    col("item.target.id").alias("work_item_id"),
    col("item.source.id").alias("parent_work_item_id"),
    current_timestamp().alias("processed_at")
)

df_sprint_items.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_sprint_work_items")
print(f"Silver sprint work items: {df_sprint_items.count()} records")

# ---- Cell 6: Transform Backlog Levels ----
df_bl_raw = spark.table(f"{bronze_lakehouse}.bronze_backlog_levels")

df_backlogs = df_bl_raw.withColumn("parsed", from_json(col("raw_json"), backlog_schema)).select(
    col("backlog_id"),
    col("backlog_name"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    col("parsed.rank").alias("rank"),
    col("parsed.type").alias("backlog_type"),
    col("parsed.workItemTypes").alias("work_item_types_json"),
    current_timestamp().alias("processed_at")
)

df_backlogs.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_backlog_levels")
print(f"Silver backlog levels: {df_backlogs.count()} records")

# ---- Cell 7: Transform Board Columns ----
df_bc_raw = spark.table(f"{bronze_lakehouse}.bronze_board_columns")

df_board_cols = df_bc_raw.select(
    col("board_name"),
    col("board_id"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    explode(from_json(col("columns_json"), ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("itemLimit", IntegerType()),
            StructField("isSplit", BooleanType()),
            StructField("columnType", StringType()),
            StructField("stateMappings", StringType())
        ])
    ))).alias("column")
).select(
    col("board_name"),
    col("board_id"),
    col("team_name"),
    col("project_name"),
    col("project_id"),
    col("column.id").alias("column_id"),
    col("column.name").alias("column_name"),
    col("column.itemLimit").alias("wip_limit"),
    col("column.isSplit").alias("is_split"),
    col("column.columnType").alias("column_type"),
    current_timestamp().alias("processed_at")
)

df_board_cols.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_board_columns")
print(f"Silver board columns: {df_board_cols.count()} records")

print("\nSilver boards transformation complete!")
