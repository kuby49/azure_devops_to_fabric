# Fabric notebook source
# Gold Layer: Build Fact Tables
# Creates analytical fact tables from Silver data for reporting

# METADATA
# {"default_lakehouse": "ado_gold", "default_lakehouse_name": "ado_gold", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
silver_lakehouse = "ado_silver"
target_lakehouse = "ado_gold"

# ---- Cell 1: Setup ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    when, lit, current_timestamp, countDistinct,
    coalesce, round as _round
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# ---- Cell 2: fact_work_items(current state snapshot) ----
try:
    df_work_items = spark.table(f"{silver_lakehouse}.silver_work_items")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping - silver_work_items table not found. Run Silver layer first.")
        mssparkutils.notebook.exit("Silver tables not found - skipping")
    raise

fact_work_items = df_work_items.select(
    col("work_item_id"),
    col("project_name"),
    col("work_item_type"),
    col("state"),
    col("assigned_to"),
    col("assigned_to_email"),
    col("iteration_path"),
    col("area_path"),
    col("story_points"),
    col("priority"),
    col("parent_id"),
    col("board_column"),
    col("board_column_done"),
    col("backlog_priority"),
    col("created_date"),
    col("changed_date"),
    col("closed_date"),
    col("state_change_date"),
    col("is_closed"),
    col("tags"),
    col("age_days"),
    col("cycle_time_days"),
    current_timestamp().alias("refreshed_at")
)

fact_work_items.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_work_items")
print(f"fact_work_items: {fact_work_items.count()} records")

# ---- Cell 3: fact_sprint_metrics ----
try:
    df_iterations = spark.table(f"{silver_lakehouse}.silver_iterations")
    df_sprint_items = spark.table(f"{silver_lakehouse}.silver_sprint_work_items")

    # Join sprint work items with work item details to calculate metrics
    df_sprint_wi = df_sprint_items.join(
        df_work_items,
        on=["work_item_id", "project_name"],
        how="left"
    )

    # Aggregate per sprint
    fact_sprint_metrics = df_sprint_wi.groupBy(
        df_sprint_items["iteration_id"],
        df_sprint_items["team_name"],
        df_sprint_items["project_name"]
    ).agg(
        count("work_item_id").alias("total_items"),
        _sum(when(col("is_closed"), 1).otherwise(0)).alias("completed_items"),
        _sum(when(~col("is_closed"), 1).otherwise(0)).alias("incomplete_items"),
        coalesce(_sum(col("story_points")), lit(0)).alias("total_story_points"),
        coalesce(_sum(when(col("is_closed"), col("story_points"))), lit(0)).alias("completed_story_points"),
        avg(col("cycle_time_days")).alias("avg_cycle_time_days"),
        countDistinct("assigned_to").alias("team_members_active")
    )

    # Join with iteration details for dates
    fact_sprint_metrics = fact_sprint_metrics.join(
        df_iterations.select("iteration_id", "iteration_name", "start_date", "finish_date", "time_frame"),
        on="iteration_id",
        how="left"
    ).withColumn(
        "velocity", col("completed_story_points")
    ).withColumn(
        "completion_rate", 
        _round(col("completed_items") / when(col("total_items") > 0, col("total_items")).otherwise(1) * 100, 1)
    ).withColumn(
        "scope_change_pct",
        lit(None).cast("double")
    ).withColumn("refreshed_at", current_timestamp())

    fact_sprint_metrics.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_sprint_metrics")
    print(f"fact_sprint_metrics: {fact_sprint_metrics.count()} records")

    # ---- Cell 4: fact_sprint_backlog ----
    fact_sprint_backlog = df_sprint_wi.select(
        df_sprint_items["iteration_id"],
        df_sprint_items["team_name"],
        df_sprint_items["project_name"],
        col("work_item_id"),
        col("work_item_type"),
        col("state"),
        col("story_points"),
        col("assigned_to"),
        col("is_closed"),
        col("priority"),
        current_timestamp().alias("refreshed_at")
    )

    fact_sprint_backlog.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_sprint_backlog")
    print(f"fact_sprint_backlog: {fact_sprint_backlog.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping sprint metrics/backlog - missing table: {e}")
    else:
        raise

# ---- Cell 5: fact_backlog_health ----
fact_backlog_health = df_work_items.groupBy(
    "project_name", "work_item_type"
).agg(
    count("*").alias("total_count"),
    _sum(when(col("is_closed"), 1).otherwise(0)).alias("closed_count"),
    _sum(when(~col("is_closed"), 1).otherwise(0)).alias("open_count"),
    _sum(when(col("story_points").isNull() & ~col("is_closed"), 1).otherwise(0)).alias("unestimated_count"),
    coalesce(_sum(col("story_points")), lit(0)).alias("total_story_points"),
    avg(col("age_days")).alias("avg_age_days"),
    _max(col("age_days")).alias("max_age_days")
).withColumn(
    "unestimated_pct",
    _round(col("unestimated_count") / when(col("open_count") > 0, col("open_count")).otherwise(1) * 100, 1)
).withColumn("refreshed_at", current_timestamp())

fact_backlog_health.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_backlog_health")
print(f"fact_backlog_health: {fact_backlog_health.count()} records")

# ---- Cell 6: fact_commits ----
try:
    df_commits = spark.table(f"{silver_lakehouse}.silver_commits")

    fact_commits = df_commits.select(
        col("commit_id"),
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("author_name"),
        col("author_email"),
        col("author_date"),
        col("message"),
        col("files_added"),
        col("files_edited"),
        col("files_deleted"),
        col("total_files_changed"),
        current_timestamp().alias("refreshed_at")
    )

    fact_commits.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_commits")
    print(f"fact_commits: {fact_commits.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping fact_commits - missing table: {e}")
    else:
        raise

# ---- Cell 7: fact_pull_requests ----
try:
    df_prs = spark.table(f"{silver_lakehouse}.silver_pull_requests")

    fact_pull_requests = df_prs.select(
        col("pr_id"),
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("title"),
        col("status"),
        col("created_by"),
        col("created_by_email"),
        col("created_date"),
        col("closed_date"),
        col("source_branch"),
        col("target_branch"),
        col("merge_status"),
        col("is_draft"),
        col("is_merged"),
        col("cycle_time_days"),
        current_timestamp().alias("refreshed_at")
    )

    fact_pull_requests.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_pull_requests")
    print(f"fact_pull_requests: {fact_pull_requests.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping fact_pull_requests - missing table: {e}")
    else:
        raise

# ---- Cell 8: fact_board_flow ----
try:
    df_board_cols = spark.table(f"{silver_lakehouse}.silver_board_columns")

    fact_board_flow = df_work_items.groupBy(
        "project_name", "board_column"
    ).agg(
        count("*").alias("item_count"),
        _sum(col("story_points")).alias("total_story_points"),
        countDistinct("assigned_to").alias("assignee_count"),
        avg("age_days").alias("avg_age_days")
    ).where(
        col("board_column").isNotNull()
    ).withColumn("refreshed_at", current_timestamp())

    fact_board_flow.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.fact_board_flow")
    print(f"fact_board_flow: {fact_board_flow.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"Skipping fact_board_flow - missing table: {e}")
    else:
        raise

print("\nGold fact tables complete!")
