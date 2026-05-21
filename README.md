# Azure DevOps to Microsoft Fabric

Medallion architecture (Bronze → Silver → Gold) for ingesting Azure DevOps data into Microsoft Fabric Lakehouses. Enables product analytics by combining engineering lifecycle data (work items, sprints, repos) into a star schema optimized for Power BI, SQL queries, and Databricks sharing.

## Architecture

```
Azure DevOps REST API (all projects)
  ├── Boards: Work Items, Sprints, Backlog, Board Columns
  └── Repos: Repositories, Commits, Pull Requests
         │
         ▼  (batch ingestion - daily)
┌────────────────────────────────────────────────────────┐
│ Fabric Workspace: AzureDevOps_Analytics                │
│                                                        │
│  ado_bronze ──▶ ado_silver ──▶ ado_gold               │
│  (raw JSON)    (normalized)    (star schema)           │
│                                     │                  │
│                                     ├─▶ Power BI       │
│                                     ├─▶ SQL Endpoint   │
│                                     └─▶ Databricks     │
└────────────────────────────────────────────────────────┘
```

## Fabric Resources

| Resource | ID |
|----------|-----|
| Workspace | `00000000-0000-0000-0000-000000000001` |
| Bronze Lakehouse (ado_bronze) | `00000000-0000-0000-0000-000000000007` |
| Silver Lakehouse (ado_silver) | `00000000-0000-0000-0000-000000000008` |
| Gold Lakehouse (ado_gold) | `00000000-0000-0000-0000-000000000009` |

## Notebooks

### Bronze (Ingestion)
| Notebook | Description |
|----------|-------------|
| `01_ingest_work_items.py` | Work items from all projects (incremental via ChangedDate) |
| `02_ingest_boards.py` | Sprints, capacity, backlog levels, board columns |
| `03_ingest_repos.py` | Repositories, commits, pull requests (incremental) |

### Silver (Transformation)
| Notebook | Description |
|----------|-------------|
| `01_transform_work_items.py` | Flatten JSON → typed columns, derive cycle time |
| `02_transform_boards.py` | Normalize iterations, capacity, board columns |
| `03_transform_repos.py` | Normalize commits, PRs with cycle time calc |

### Gold (Analytics)
| Notebook | Description |
|----------|-------------|
| `01_build_facts.py` | fact_work_items, fact_sprint_metrics, fact_backlog_health, fact_commits, fact_pull_requests, fact_board_flow |
| `02_build_dimensions.py` | dim_project, dim_iteration, dim_team_member, dim_repository, dim_board, dim_date |

## Setup

### Prerequisites
1. Azure DevOps PAT with read access to Work Items, Code, and Project/Team
2. Fabric Variable Library (`ADO_Config`) with the PAT stored as a secret (already configured)
3. Fabric capacity (F2+ for dev, F16+ for production)

### Quick Start
1. The ADO PAT is already stored in the `ADO_Config` Variable Library in the workspace
2. Deploy notebooks to the workspace (or use pre-deployed ones)
3. Run notebooks in order: Bronze → Silver → Gold
4. (Optional) To rotate the PAT, update the `ado_pat` secret in the Variable Library

## Data Model (Gold Layer)

### Fact Tables
- **fact_work_items** — Current state snapshot of all work items
- **fact_sprint_metrics** — Sprint velocity, completion rate, cycle time
- **fact_sprint_backlog** — Items committed per sprint
- **fact_backlog_health** — Backlog aging, estimation coverage by type
- **fact_board_flow** — Board column distribution and WIP
- **fact_commits** — Commit activity and file changes
- **fact_pull_requests** — PR lifecycle and review metrics

### Dimension Tables
- **dim_project** — Project reference
- **dim_iteration** — Sprint/iteration with dates
- **dim_team_member** — People with team/capacity
- **dim_repository** — Repo metadata
- **dim_board** — Board columns and WIP limits
- **dim_date** — Calendar (2020–2027)

## Databricks Integration

Gold layer tables are stored as Delta Lake in OneLake. Access from Databricks via:
- **OneLake Shortcuts** — zero-copy access
- **Unity Catalog** — mount as external tables
