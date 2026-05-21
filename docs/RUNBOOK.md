# Operations Runbook

## Daily Pipeline Schedule

| Property | Value |
|----------|-------|
| Pipeline | `ADO_Ingestion_Pipeline` |
| Schedule | Daily at 06:00 UTC |
| Schedule ID | `5709df60-4d33-4b27-96b5-27bb2f436fdb` |
| Pipeline ID | `d716de0f-9f34-4fa3-bbab-6baaeed7a9af` |

## Pipeline Execution Flow

```
Bronze (parallel)                Silver (depends on Bronze)        Gold (depends on Silver)
┌─────────────────────┐         ┌──────────────────────────┐     ┌─────────────────────┐
│ Ingest Work Items   │────────▶│ Transform Work Items     │──┐  │                     │
│ Ingest Boards       │────────▶│ Transform Boards         │──┼─▶│ Build Facts         │
│ Ingest Repos        │────────▶│ Transform Repos          │──┘  │ Build Dimensions    │
└─────────────────────┘         └──────────────────────────┘     └─────────────────────┘
```

## Prerequisites Before First Run

1. **ADO PAT (already configured)**: A PAT named `Fabric-ADO-Ingestion` is stored in the Fabric Variable Library `ADO_Config`
   - Scopes: Work Items (read), Code (read), Project & Team (read)
   - Valid until: May 2027
   - Variable Library ID: `ad2ec3e5-0c1b-4612-82d1-df6b91800165`

2. **Variable Library**: The `ADO_Config` Variable Library in the workspace contains:
   - `ado_pat` (Secret) — Azure DevOps Personal Access Token
   - `ado_org_url` (String) — `https://dev.azure.com/your-org`

3. **Fallback (Key Vault)**: If Variable Library is unavailable, notebooks fall back to Azure Key Vault:
   - Vault: `https://your-keyvault.vault.azure.net/`
   - Secret: `ado-pat`
   - Requires: Key Vault Secrets Officer RBAC role + Fabric KV connection

4. **ADO PAT permissions needed**:
   - Work Items: Read
   - Code: Read
   - Project and Team: Read

## Monitoring

### Check pipeline run status
```powershell
$env:AZURE_CONFIG_DIR = "$HOME\.azure-mcap"
$wsId = "00000000-0000-0000-0000-000000000001"
$pipelineId = "d716de0f-9f34-4fa3-bbab-6baaeed7a9af"

az rest --method get --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$pipelineId/jobs/instances?limit=5" `
  --output table
```

### Trigger manual pipeline run
```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$pipelineId/jobs/instances?jobType=Pipeline" `
  --body '{}'
```

## Resource Inventory

| Resource | Type | ID |
|----------|------|-----|
| AzureDevOps_Analytics | Workspace | `00000000-0000-0000-0000-000000000001` |
| ado_bronze | Lakehouse | `00000000-0000-0000-0000-000000000007` |
| ado_silver | Lakehouse | `00000000-0000-0000-0000-000000000008` |
| ado_gold | Lakehouse | `00000000-0000-0000-0000-000000000009` |
| ADO_Config | VariableLibrary | `ad2ec3e5-0c1b-4612-82d1-df6b91800165` |
| ADO_Ingestion_Pipeline | DataPipeline | `d716de0f-9f34-4fa3-bbab-6baaeed7a9af` |
| 01_Bronze_Ingest_WorkItems | Notebook | `3c0c5473-40f3-415b-874b-522de6ff7cd8` |
| 02_Bronze_Ingest_Boards | Notebook | `381b4883-fdfa-4a82-ac8f-efffb87d12d5` |
| 03_Bronze_Ingest_Repos | Notebook | `dedbaef5-c28b-4ffa-bedc-b40c37548cec` |
| 04_Silver_Transform_WorkItems | Notebook | `58666d7e-391f-4089-844a-f9ba5f437133` |
| 05_Silver_Transform_Boards | Notebook | `7ad4dd02-97e1-4473-91bc-7cd877c3f947` |
| 06_Silver_Transform_Repos | Notebook | `4208092f-22ff-4ed6-8230-bcba8e7f7174` |
| 07_Gold_Build_Facts | Notebook | `191de556-dc5c-496c-9fae-bd85f5309c5d` |
| 08_Gold_Build_Dimensions | Notebook | `db06055e-d31f-4152-a975-b636b73aa5cc` |

## Incremental Load Strategy

| Entity | Watermark | Method |
|--------|-----------|--------|
| Work Items | `System.ChangedDate` | WIQL filter + upsert |
| Commits | `author.date` | `fromDate` API parameter |
| Pull Requests | `creationDate` | Client-side filter |
| Boards/Sprints | N/A | Full overwrite (small dataset) |

## Troubleshooting

### Pipeline fails at Bronze layer
- Check Variable Library `ADO_Config` exists and contains `ado_pat` secret
- Verify PAT hasn't expired (current expiry: May 2027)
- Check ADO org URL is accessible
- If Variable Library fails, ensure Key Vault fallback is configured

### Pipeline fails at Silver layer
- Verify Bronze tables have data: query via SQL endpoint
- Check for schema changes in ADO API responses

### Pipeline fails at Gold layer
- Verify Silver tables exist and have data
- Check for null key columns breaking joins

## Databricks Access

Gold tables accessible via ABFS:
```
abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/ado_gold/Tables/<table-name>
```

Available tables: `fact_work_items`, `fact_sprint_metrics`, `fact_sprint_backlog`, `fact_backlog_health`, `fact_board_flow`, `fact_commits`, `fact_pull_requests`, `dim_project`, `dim_iteration`, `dim_team_member`, `dim_repository`, `dim_board`, `dim_date`
