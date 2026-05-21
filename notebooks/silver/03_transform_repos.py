# Fabric notebook source
# Silver Layer: Transform Repos Data (Commits, Pull Requests)
# Normalizes Bronze repos data into structured Silver tables

# METADATA
# {"default_lakehouse": "ado_silver", "default_lakehouse_name": "ado_silver", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
bronze_lakehouse = "ado_bronze"
target_lakehouse = "ado_silver"

# ---- Cell 1: Setup ----
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    datediff, when, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

# Schemas for ADO repos JSON
repo_schema = StructType([
    StructField("url", StringType()),
    StructField("webUrl", StringType()),
    StructField("isFork", BooleanType()),
    StructField("isDisabled", BooleanType())
])

commit_schema = StructType([
    StructField("author", StructType([
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("date", StringType())
    ])),
    StructField("committer", StructType([
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("date", StringType())
    ])),
    StructField("comment", StringType()),
    StructField("changeCounts", StructType([
        StructField("Add", IntegerType()),
        StructField("Edit", IntegerType()),
        StructField("Delete", IntegerType())
    ])),
    StructField("url", StringType())
])

pr_schema = StructType([
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("status", StringType()),
    StructField("createdBy", StructType([
        StructField("displayName", StringType()),
        StructField("uniqueName", StringType())
    ])),
    StructField("creationDate", StringType()),
    StructField("closedDate", StringType()),
    StructField("sourceRefName", StringType()),
    StructField("targetRefName", StringType()),
    StructField("mergeStatus", StringType()),
    StructField("isDraft", BooleanType()),
    StructField("reviewers", StringType())
])

# ---- Cell 2: Transform Repositories ----
try:
    df_repos_raw = spark.table(f"{bronze_lakehouse}.bronze_repositories")

    df_repos = df_repos_raw.withColumn("parsed", from_json(col("raw_json"), repo_schema)).select(
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("project_id"),
        col("default_branch"),
        col("size").cast("long"),
        col("parsed.url").alias("url"),
        col("parsed.webUrl").alias("web_url"),
        col("parsed.isFork").alias("is_fork"),
        col("parsed.isDisabled").alias("is_disabled"),
        current_timestamp().alias("processed_at")
    )

    df_repos.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_repositories")
    print(f"Silver repositories: {df_repos.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping repositories - bronze table not found")
    else:
        raise

# ---- Cell 3: Transform Commits ----
try:
    df_commits_raw = spark.table(f"{bronze_lakehouse}.bronze_commits")

    df_commits = df_commits_raw.withColumn("parsed", from_json(col("raw_json"), commit_schema)).select(
        col("commit_id"),
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("project_id"),
        col("parsed.author.name").alias("author_name"),
        col("parsed.author.email").alias("author_email"),
        to_timestamp(col("parsed.author.date")).alias("author_date"),
        col("parsed.committer.name").alias("committer_name"),
        col("parsed.committer.email").alias("committer_email"),
        to_timestamp(col("parsed.committer.date")).alias("committer_date"),
        col("parsed.comment").alias("message"),
        col("parsed.changeCounts.Add").alias("files_added"),
        col("parsed.changeCounts.Edit").alias("files_edited"),
        col("parsed.changeCounts.Delete").alias("files_deleted"),
        col("parsed.url").alias("url"),
        current_timestamp().alias("processed_at")
    )

    df_commits = df_commits.withColumn(
        "total_files_changed",
        coalesce(col("files_added"), lit(0)) + 
        coalesce(col("files_edited"), lit(0)) + 
        coalesce(col("files_deleted"), lit(0))
    )

    df_commits.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_commits")
    print(f"Silver commits: {df_commits.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping commits - bronze table not found")
    else:
        raise

# ---- Cell 4: Transform Pull Requests ----
try:
    df_prs_raw = spark.table(f"{bronze_lakehouse}.bronze_pull_requests")

    df_prs = df_prs_raw.withColumn("parsed", from_json(col("raw_json"), pr_schema)).select(
        col("pr_id").cast("int"),
        col("repo_id"),
        col("repo_name"),
        col("project_name"),
        col("project_id"),
        col("parsed.title").alias("title"),
        col("parsed.description").alias("description"),
        col("parsed.status").alias("status"),
        col("parsed.createdBy.displayName").alias("created_by"),
        col("parsed.createdBy.uniqueName").alias("created_by_email"),
        to_timestamp(col("parsed.creationDate")).alias("created_date"),
        to_timestamp(col("parsed.closedDate")).alias("closed_date"),
        col("parsed.sourceRefName").alias("source_branch"),
        col("parsed.targetRefName").alias("target_branch"),
        col("parsed.mergeStatus").alias("merge_status"),
        col("parsed.isDraft").alias("is_draft"),
        col("parsed.reviewers").alias("reviewers_json"),
        current_timestamp().alias("processed_at")
    )

    df_prs = df_prs.withColumn(
        "cycle_time_days",
        when(col("closed_date").isNotNull(),
             datediff(col("closed_date"), col("created_date"))
        ).otherwise(None)
    ).withColumn(
        "is_merged",
        col("status") == "completed"
    )

    df_prs.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.silver_pull_requests")
    print(f"Silver pull requests: {df_prs.count()} records")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print("Skipping pull requests - bronze table not found")
    else:
        raise

print("\nSilver repos transformation complete!")
