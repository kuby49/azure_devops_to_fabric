# Fabric notebook source
# Bronze Layer: Ingest Boards Data (Sprints, Backlog, Board Columns)
# Extracts iterations, sprint capacity, sprint work items, backlog levels, and board columns
# from all projects in Azure DevOps

# METADATA
# {"default_lakehouse": "ado_bronze", "default_lakehouse_name": "ado_bronze", "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000001"}

# PARAMETERS
# (none - config from Variable Library)
target_lakehouse = "ado_bronze"

# ---- Cell 1: Configuration ----
import requests
import json
import base64
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from notebookutils import mssparkutils

# Enable high concurrency session sharing
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")

API_VERSION = "7.1"
ADO_ORG_URL = "https://dev.azure.com/your-org"

# Get PAT from Key Vault via Fabric connection
pat = mssparkutils.credentials.getSecret("https://your-keyvault.vault.azure.net/", "ado-pat")

auth_header = {
    "Authorization": "Basic " + base64.b64encode(f":{pat}".encode()).decode(),
    "Content-Type": "application/json"
}

# ---- Cell 2: API Helpers ----
def get_all_projects():
    """Fetch all projects in the organization."""
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

def get_teams(project_name):
    """Get all teams in a project."""
    url = f"{ADO_ORG_URL}/_apis/projects/{project_name}/teams?api-version={API_VERSION}&$top=500"
    resp = requests.get(url, headers=auth_header)
    resp.raise_for_status()
    return resp.json().get("value", [])

def get_iterations(project_name, team_name):
    """Get all iterations (sprints) for a team."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/teamsettings/iterations?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

def get_iteration_capacity(project_name, team_name, iteration_id):
    """Get capacity for a specific iteration."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/teamsettings/iterations/{iteration_id}/capacities?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

def get_iteration_work_items(project_name, team_name, iteration_id):
    """Get work items assigned to a specific iteration."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/teamsettings/iterations/{iteration_id}/workitems?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("workItemRelations", [])
    return []

def get_backlog_levels(project_name, team_name):
    """Get backlog configuration (levels, work item types)."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/backlogs?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

def get_board_columns(project_name, team_name, board_name):
    """Get board columns and their configuration."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/boards/{board_name}/columns?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

def get_boards(project_name, team_name):
    """Get all boards for a team."""
    url = f"{ADO_ORG_URL}/{project_name}/{team_name}/_apis/work/boards?api-version={API_VERSION}"
    resp = requests.get(url, headers=auth_header)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []

# ---- Cell 3: Extract All Boards Data ----
ingested_at = datetime.utcnow().isoformat()

projects = get_all_projects()
print(f"Found {len(projects)} projects")

all_iterations = []
all_capacities = []
all_sprint_work_items = []
all_backlog_levels = []
all_board_columns = []
all_teams = []

for project in projects:
    project_name = project["name"]
    project_id = project["id"]
    print(f"\nProcessing project: {project_name}")
    
    teams = get_teams(project_name)
    print(f"  Teams: {len(teams)}")
    
    for team in teams:
        team_name = team["name"]
        team_id = team["id"]
        
        # Store team info
        all_teams.append({
            "team_id": team_id,
            "team_name": team_name,
            "project_name": project_name,
            "project_id": project_id,
            "raw_json": json.dumps(team),
            "ingested_at": ingested_at
        })
        
        # Iterations (Sprints)
        iterations = get_iterations(project_name, team_name)
        for iteration in iterations:
            iter_id = iteration.get("id", "")
            all_iterations.append({
                "iteration_id": iter_id,
                "iteration_name": iteration.get("name", ""),
                "iteration_path": iteration.get("path", ""),
                "team_name": team_name,
                "team_id": team_id,
                "project_name": project_name,
                "project_id": project_id,
                "raw_json": json.dumps(iteration),
                "ingested_at": ingested_at
            })
            
            # Sprint capacity
            capacities = get_iteration_capacity(project_name, team_name, iter_id)
            for cap in capacities:
                all_capacities.append({
                    "iteration_id": iter_id,
                    "team_name": team_name,
                    "project_name": project_name,
                    "project_id": project_id,
                    "raw_json": json.dumps(cap),
                    "ingested_at": ingested_at
                })
            
            # Sprint work items
            sprint_items = get_iteration_work_items(project_name, team_name, iter_id)
            if sprint_items:
                all_sprint_work_items.append({
                    "iteration_id": iter_id,
                    "team_name": team_name,
                    "project_name": project_name,
                    "project_id": project_id,
                    "raw_json": json.dumps(sprint_items),
                    "ingested_at": ingested_at
                })
        
        # Backlog levels
        backlogs = get_backlog_levels(project_name, team_name)
        for backlog in backlogs:
            all_backlog_levels.append({
                "backlog_id": backlog.get("id", ""),
                "backlog_name": backlog.get("name", ""),
                "team_name": team_name,
                "project_name": project_name,
                "project_id": project_id,
                "raw_json": json.dumps(backlog),
                "ingested_at": ingested_at
            })
        
        # Boards and columns
        boards = get_boards(project_name, team_name)
        for board in boards:
            board_name = board.get("name", "")
            columns = get_board_columns(project_name, team_name, board.get("id", board_name))
            all_board_columns.append({
                "board_name": board_name,
                "board_id": board.get("id", ""),
                "team_name": team_name,
                "project_name": project_name,
                "project_id": project_id,
                "columns_json": json.dumps(columns),
                "board_json": json.dumps(board),
                "ingested_at": ingested_at
            })

print(f"\n--- Summary ---")
print(f"Teams: {len(all_teams)}")
print(f"Iterations: {len(all_iterations)}")
print(f"Capacity records: {len(all_capacities)}")
print(f"Sprint work item sets: {len(all_sprint_work_items)}")
print(f"Backlog levels: {len(all_backlog_levels)}")
print(f"Board configs: {len(all_board_columns)}")

# ---- Cell 4: Write to Bronze Delta Tables ----
def write_to_bronze(records, table_name):
    """Write records to a Bronze Delta table (full overwrite each run)."""
    if records:
        df = spark.createDataFrame(records)
        full_table_name = f"{target_lakehouse}.{table_name}"
        df.write.mode("overwrite").format("delta").saveAsTable(full_table_name)
        print(f"Wrote {len(records)} records to {full_table_name}")
    else:
        print(f"No records for {table_name}")

write_to_bronze(all_teams, "bronze_teams")
write_to_bronze(all_iterations, "bronze_iterations")
write_to_bronze(all_capacities, "bronze_sprint_capacity")
write_to_bronze(all_sprint_work_items, "bronze_sprint_work_items")
write_to_bronze(all_backlog_levels, "bronze_backlog_levels")
write_to_bronze(all_board_columns, "bronze_board_columns")

print("\nBronze boards ingestion complete!")
