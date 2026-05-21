# Fabric notebook source
# Gold Layer: Build Dimension Tables
# Creates dimension tables for the star schema

# METADATA
# {"default_lakehouse": "ado_gold", "default_lakehouse_name": "ado_gold", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
silver_lakehouse = "ado_silver"
target_lakehouse = "ado_gold"

# ---- Cell 1: Setup ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    date_format, dayofweek, dayofmonth, month, year, quarter,
    weekofyear, concat_ws, expr, sequence, explode, when
)
from pyspark.sql.types import DateType
from datetime import date

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# ---- Cell 2: dim_project----
try:
    df_work_items = spark.table(f"{silver_lakehouse}.silver_work_items")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping - silver_work_items table not found. Run Silver layer first.")
        mssparkutils.notebook.exit("Silver tables not found - skipping")
    raise

try:
    df_repos = spark.table(f"{silver_lakehouse}.silver_repositories")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        df_repos = None
    else:
        raise

# Get unique projects from work items
dim_project = df_work_items.select(
    col("project_name"),
    col("project_id")
).distinct().withColumn("refreshed_at", current_timestamp())

dim_project.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_project")
print(f"dim_project: {dim_project.count()} records")

# ---- Cell 3: dim_iteration (Sprint dimension) ----
try:
    df_iterations = spark.table(f"{silver_lakehouse}.silver_iterations")

    dim_iteration = df_iterations.select(
        col("iteration_id"),
        col("iteration_name"),
        col("iteration_path"),
        col("team_name"),
        col("team_id"),
        col("project_name"),
        col("project_id"),
        col("start_date"),
        col("finish_date"),
        col("time_frame"),
        current_timestamp().alias("refreshed_at")
    ).distinct()

    dim_iteration.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_iteration")
    print(f"dim_iteration: {dim_iteration.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping dim_iteration - missing table: {e}")
    else:
        raise

# ---- Cell 4: dim_team_member ----
try:
    df_capacity = spark.table(f"{silver_lakehouse}.silver_sprint_capacity")

    dim_team_member = df_capacity.select(
        col("member_name"),
        col("member_email"),
        col("team_name"),
        col("project_name"),
        col("activity")
    ).distinct().withColumn(
        "member_id", monotonically_increasing_id()
    ).withColumn("refreshed_at", current_timestamp())

    # Also include assigned_to from work items who may not be in capacity
    df_wi_members = df_work_items.select(
        col("assigned_to").alias("member_name"),
        col("assigned_to_email").alias("member_email"),
        col("project_name")
    ).where(col("assigned_to").isNotNull()).distinct()

    dim_team_member = dim_team_member.unionByName(
        df_wi_members.withColumn("team_name", lit(None).cast("string"))
                     .withColumn("activity", lit(None).cast("string"))
                     .withColumn("member_id", monotonically_increasing_id())
                     .withColumn("refreshed_at", current_timestamp()),
        allowMissingColumns=True
    ).dropDuplicates(["member_email", "project_name"])

    dim_team_member.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_team_member")
    print(f"dim_team_member: {dim_team_member.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        # Build dim_team_member from work items only
        df_wi_members = df_work_items.select(
            col("assigned_to").alias("member_name"),
            col("assigned_to_email").alias("member_email"),
            col("project_name")
        ).where(col("assigned_to").isNotNull()).distinct().withColumn(
            "team_name", lit(None).cast("string")
        ).withColumn("activity", lit(None).cast("string")
        ).withColumn("member_id", monotonically_increasing_id()
        ).withColumn("refreshed_at", current_timestamp())

        df_wi_members.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_team_member")
        print(f"dim_team_member (from work items only): {df_wi_members.count()} records")
    else:
        raise

# ---- Cell 5: dim_repository ----
if df_repos is not None:
    dim_repository = df_repos.select(
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("project_id"),
        col("default_branch"),
        col("size"),
        col("is_fork"),
        col("is_disabled"),
        current_timestamp().alias("refreshed_at")
    )

    dim_repository.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_repository")
    print(f"dim_repository: {dim_repository.count()} records")
else:
    print("Skipping dim_repository - silver_repositories not found")

# ---- Cell 6: dim_board ----
try:
    df_board_cols = spark.table(f"{silver_lakehouse}.silver_board_columns")

    dim_board = df_board_cols.select(
        col("board_id"),
        col("board_name"),
        col("team_name"),
        col("project_name"),
        col("project_id"),
        col("column_id"),
        col("column_name"),
        col("wip_limit"),
        col("is_split"),
        col("column_type"),
        current_timestamp().alias("refreshed_at")
    )

    dim_board.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_board")
    print(f"dim_board: {dim_board.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping dim_board - missing table: {e}")
    else:
        raise

# ---- Cell 7: dim_date ----
# Generate a date dimension from 2020-01-01 to 2027-12-31
start_date = date(2020, 1, 1)
end_date = date(2027, 12, 31)

df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        interval 1 day
    )) AS date_key
""")

dim_date = df_dates.select(
    col("date_key"),
    year("date_key").alias("year"),
    quarter("date_key").alias("quarter"),
    month("date_key").alias("month"),
    dayofmonth("date_key").alias("day"),
    weekofyear("date_key").alias("week_of_year"),
    dayofweek("date_key").alias("day_of_week"),
    date_format("date_key", "EEEE").alias("day_name"),
    date_format("date_key", "MMMM").alias("month_name"),
    date_format("date_key", "yyyy-MM").alias("year_month"),
    date_format("date_key", "yyyy-'Q'Q").alias("year_quarter"),
    when(dayofweek("date_key").isin(1, 7), True).otherwise(False).alias("is_weekend")
)

dim_date.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.dim_date")
print(f"dim_date: {dim_date.count()} records")

print("\nGold dimension tables complete!")
