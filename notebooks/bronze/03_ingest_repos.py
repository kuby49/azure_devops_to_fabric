# Fabric notebook source
# Bronze Layer: Ingest Repos, Commits, and Pull Requests from Azure DevOps
# Extracts repository metadata, commits, and PRs from all projects
# Supports incremental loads via date watermarks

# METADATA
# {"default_lakehouse": "ado_bronze", "default_lakehouse_name": "ado_bronze", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
full_load = False
commits_days_back = 30
target_lakehouse = "ado_bronze"

# ---- Cell 1: Configuration ----
import requests
import json
import base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from notebookutils import mssparkutils

# Enable high concurrency session sharing
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

API_VERSION = "7.1"
WATERMARK_TABLE = f"{target_lakehouse}.ingestion_watermarks"
ADO_ORG_URL = "https://dev.azure.com/your-org"

# Get PAT from Key Vault via Fabric connection
pat = mssparkutils.credentials.getSecret("https://your-keyvault.vault.azure.net/", "ado-pat")

auth_header = {
    "Authorization": "Basic " + base64.b64encode(f":{pat}".encode()).decode(),
    "Content-Type": "application/json"
}

# ---- Cell 2: Watermark Management ----
def get_watermark(spark, entity_name):
    """Get the last ingestion watermark."""
    try:
        df = spark.sql(f"SELECT max_changed_date FROM {WATERMARK_TABLE} WHERE entity = '{entity_name}'")
        if df.count() > 0:
            return df.collect()[0]["max_changed_date"]
    except Exception:
        pass
    return None

def update_watermark(spark, entity_name, max_date):
    """Update the watermark after successful ingestion."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            entity STRING, max_changed_date STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT '{entity_name}' AS entity, '{max_date}' AS max_changed_date) AS source
        ON target.entity = source.entity
        WHEN MATCHED THEN UPDATE SET max_changed_date = source.max_changed_date
        WHEN NOT MATCHED THEN INSERT (entity, max_changed_date) VALUES (source.entity, source.max_changed_date)
    """)

# ---- Cell 3: API Helpers ----
def get_all_projects():
    """Fetch all projects."""
    url = f"{ADO_ORG_URL}/_apis/projects?api-version={API_VERSION}&$top=500"
    projects = []
    while url:
        resp = requests.get(url, headers=auth_header)
        resp.raise_for_status()
        data = resp.json()
        projects.extend(data["value"])
        cont = data.get("continuationToken")
        url = f"{ADO_ORG_URL}/_apis/projects?api-version={API_VERSION}&$top=500&continuationToken={cont}" if cont else None
    return projects

def get_repositories(project_name):
    """Get all repositories in a project."""
    url = f"{ADO_ORG_URL}/{project_name}/_apis/git/repositories?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    resp.raise_for_status()
    return resp.json().get("value", [])

def get_commits(project_name, repo_id, from_date=None, top=1000):
    """Get commits from a repository, optionally filtered by date."""
    url = f"{ADO_ORG_URL}/{project_name}/_apis/git/repositories/{repo_id}/commits?api-version={API_VERSION}&$top={top}"
    if from_date:
        url += f"&searchCriteria.fromDate={from_date}"
    
    all_commits = []
    while url:
        resp = requests.get(url, headers=auth_header)
        if resp.status_code != 200:
            break
        data = resp.json()
        all_commits.extend(data.get("value", []))
        # Handle pagination via skip
        if len(data.get("value", [])) == top:
            skip = len(all_commits)
            url = f"{ADO_ORG_URL}/{project_name}/_apis/git/repositories/{repo_id}/commits?api-version={API_VERSION}&$top={top}&$skip={skip}"
            if from_date:
                url += f"&searchCriteria.fromDate={from_date}"
        else:
            url = None
    return all_commits

def get_pull_requests(project_name, repo_id, status="all", top=500):
    """Get pull requests from a repository."""
    url = f"{ADO_ORG_URL}/{project_name}/_apis/git/repositories/{repo_id}/pullrequests?api-version={API_VERSION}&searchCriteria.status={status}&$top={top}"
    all_prs = []
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        all_prs.extend(resp.json().get("value", []))
    return all_prs

# ---- Cell 4: Extract Repositories ----
ingested_at = datetime.utcnow().isoformat()

projects = get_all_projects()
print(f"Found {len(projects)} projects")

# Determine date filter for commits
commits_watermark = None
if not full_load:
    commits_watermark = get_watermark(spark, "commits")
if not commits_watermark:
    commits_watermark = (datetime.utcnow() - timedelta(days=commits_days_back)).strftime("%Y-%m-%dT00:00:00Z")
print(f"Commits from date: {commits_watermark}")

pr_watermark = None
if not full_load:
    pr_watermark = get_watermark(spark, "pull_requests")

all_repos = []
all_commits = []
all_prs = []

for project in projects:
    project_name = project["name"]
    project_id = project["id"]
    print(f"\nProcessing project: {project_name}")
    
    try:
        repos = get_repositories(project_name)
        print(f"  Repositories: {len(repos)}")
        
        for repo in repos:
            repo_id = repo["id"]
            repo_name = repo.get("name", "")
            
            # Store repo metadata
            all_repos.append({
                "repo_id": repo_id,
                "repo_name": repo_name,
                "project_name": project_name,
                "project_id": project_id,
                "default_branch": repo.get("defaultBranch", ""),
                "size": repo.get("size", 0),
                "raw_json": json.dumps(repo),
                "ingested_at": ingested_at
            })
            
            # Get commits
            commits = get_commits(project_name, repo_id, from_date=commits_watermark)
            for commit in commits:
                all_commits.append({
                    "commit_id": commit.get("commitId", ""),
                    "repo_id": repo_id,
                    "repo_name": repo_name,
                    "project_name": project_name,
                    "project_id": project_id,
                    "raw_json": json.dumps(commit),
                    "ingested_at": ingested_at
                })
            
            # Get PRs
            prs = get_pull_requests(project_name, repo_id)
            for pr in prs:
                # Filter by creation date if incremental
                if pr_watermark:
                    pr_date = pr.get("creationDate", "")
                    if pr_date < pr_watermark:
                        continue
                all_prs.append({
                    "pr_id": pr.get("pullRequestId", 0),
                    "repo_id": repo_id,
                    "repo_name": repo_name,
                    "project_name": project_name,
                    "project_id": project_id,
                    "raw_json": json.dumps(pr),
                    "ingested_at": ingested_at
                })
            
            if commits or prs:
                print(f"    {repo_name}: {len(commits)} commits, {len([p for p in all_prs if p['repo_id'] == repo_id])} PRs")
    except Exception as e:
        print(f"  Error: {e}")

print(f"\n--- Summary ---")
print(f"Repositories: {len(all_repos)}")
print(f"Commits: {len(all_commits)}")
print(f"Pull Requests: {len(all_prs)}")

# ---- Cell 5: Write to Bronze Delta Tables ----
# Repositories - full overwrite (small dataset)
if all_repos:
    df_repos = spark.createDataFrame(all_repos)
    df_repos.write.mode("overwrite").format("delta").saveAsTable(f"{target_lakehouse}.bronze_repositories")
    print(f"Wrote {len(all_repos)} repositories")

# Commits - merge (incremental)
if all_commits:
    df_commits = spark.createDataFrame(all_commits)
    
    commits_table = f"{target_lakehouse}.bronze_commits"
    if full_load or not spark.catalog.tableExists(commits_table):
        df_commits.write.mode("overwrite").format("delta").saveAsTable(commits_table)
    else:
        df_commits.createOrReplaceTempView("new_commits")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {commits_table}
            USING DELTA AS SELECT * FROM new_commits WHERE 1=0
        """)
        spark.sql(f"""
            MERGE INTO {commits_table} AS target
            USING new_commits AS source
            ON target.commit_id = source.commit_id AND target.repo_id = source.repo_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"Wrote {len(all_commits)} commits")
    
    # Update commits watermark
    max_commit_date = max(
        json.loads(c["raw_json"]).get("author", {}).get("date", "1900-01-01")
        for c in all_commits
    )
    update_watermark(spark, "commits", max_commit_date)

# Pull Requests - merge (incremental)
if all_prs:
    df_prs = spark.createDataFrame(all_prs)
    
    prs_table = f"{target_lakehouse}.bronze_pull_requests"
    if full_load or not spark.catalog.tableExists(prs_table):
        df_prs.write.mode("overwrite").format("delta").saveAsTable(prs_table)
    else:
        df_prs.createOrReplaceTempView("new_prs")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {prs_table}
            USING DELTA AS SELECT * FROM new_prs WHERE 1=0
        """)
        spark.sql(f"""
            MERGE INTO {prs_table} AS target
            USING new_prs AS source
            ON target.pr_id = source.pr_id AND target.repo_id = source.repo_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"Wrote {len(all_prs)} pull requests")
    
    max_pr_date = max(
        json.loads(p["raw_json"]).get("creationDate", "1900-01-01")
        for p in all_prs
    )
    update_watermark(spark, "pull_requests", max_pr_date)

print("\nBronze repos ingestion complete!")
