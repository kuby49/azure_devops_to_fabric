# Deploy notebooks and pipeline to Fabric workspace
# Run this script after setting up AZURE_CONFIG_DIR for the correct tenant

$ErrorActionPreference = "Stop"
$env:AZURE_CONFIG_DIR = "$HOME\.azure-mcap"

$wsId = "00000000-0000-0000-0000-000000000001"
$baseUrl = "https://api.fabric.microsoft.com/v1/workspaces/$wsId"
$repoRoot = "C:\Users\<user>\azure_devops_to_fabric"

# Lakehouse IDs
$bronzeLH = "00000000-0000-0000-0000-000000000007"
$silverLH = "00000000-0000-0000-0000-000000000008"
$goldLH = "00000000-0000-0000-0000-000000000009"

function Convert-NotebookToIpynb {
    param([string]$pyFilePath, [string]$lakehouseId, [string]$lakehouseName)
    
    $content = Get-Content $pyFilePath -Raw
    
    # Split into cells by "# ---- Cell" markers
    $cellBlocks = $content -split '(?=# ---- Cell \d+:)'
    
    $cells = @()
    foreach ($block in $cellBlocks) {
        if ([string]::IsNullOrWhiteSpace($block)) { continue }
        
        # Convert block to cell source lines
        $lines = $block -split "`n" | ForEach-Object { "$_`n" }
        # Remove trailing newline from last line
        if ($lines.Count -gt 0) {
            $lines[-1] = $lines[-1].TrimEnd("`n")
        }
        
        $cell = @{
            cell_type = "code"
            source = $lines
            metadata = @{}
            outputs = @()
            execution_count = $null
        }
        $cells += $cell
    }
    
    # Build ipynb structure
    $notebook = @{
        nbformat = 4
        nbformat_minor = 5
        metadata = @{
            language_info = @{
                name = "python"
            }
            kernel_info = @{
                name = "synapse_pyspark"
            }
            dependencies = @{
                lakehouse = @{
                    default_lakehouse = $lakehouseId
                    default_lakehouse_name = $lakehouseName
                    default_lakehouse_workspace_id = $wsId
                }
            }
        }
        cells = $cells
    }
    
    return $notebook | ConvertTo-Json -Depth 10
}

function Deploy-Notebook {
    param([string]$pyFilePath, [string]$displayName, [string]$lakehouseId, [string]$lakehouseName)
    
    Write-Host "Deploying notebook: $displayName"
    
    $ipynbJson = Convert-NotebookToIpynb -pyFilePath $pyFilePath -lakehouseId $lakehouseId -lakehouseName $lakehouseName
    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ipynbJson))
    
    $body = @{
        displayName = $displayName
        type = "Notebook"
        definition = @{
            format = "ipynb"
            parts = @(
                @{
                    path = "notebook-content.ipynb"
                    payload = $encoded
                    payloadType = "InlineBase64"
                }
            )
        }
    } | ConvertTo-Json -Depth 5
    
    $body | Out-File -Encoding utf8 "C:\temp\nb_body.json" -Force
    
    $result = az rest --method post --resource "https://api.fabric.microsoft.com" `
        --url "$baseUrl/items" `
        --body "@C:\temp\nb_body.json" --output json 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: $result" -ForegroundColor Red
        return $null
    }
    
    $item = $result | ConvertFrom-Json
    Write-Host "  Created: $($item.id)"
    return $item.id
}

# Deploy Bronze notebooks
Write-Host "`n=== Deploying Bronze Notebooks ==="
$nb_bronze_wi = Deploy-Notebook "$repoRoot\notebooks\bronze\01_ingest_work_items.py" "01_Bronze_Ingest_WorkItems" $bronzeLH "ado_bronze"
Start-Sleep 2
$nb_bronze_boards = Deploy-Notebook "$repoRoot\notebooks\bronze\02_ingest_boards.py" "02_Bronze_Ingest_Boards" $bronzeLH "ado_bronze"
Start-Sleep 2
$nb_bronze_repos = Deploy-Notebook "$repoRoot\notebooks\bronze\03_ingest_repos.py" "03_Bronze_Ingest_Repos" $bronzeLH "ado_bronze"
Start-Sleep 2

# Deploy Silver notebooks
Write-Host "`n=== Deploying Silver Notebooks ==="
$nb_silver_wi = Deploy-Notebook "$repoRoot\notebooks\silver\01_transform_work_items.py" "04_Silver_Transform_WorkItems" $silverLH "ado_silver"
Start-Sleep 2
$nb_silver_boards = Deploy-Notebook "$repoRoot\notebooks\silver\02_transform_boards.py" "05_Silver_Transform_Boards" $silverLH "ado_silver"
Start-Sleep 2
$nb_silver_repos = Deploy-Notebook "$repoRoot\notebooks\silver\03_transform_repos.py" "06_Silver_Transform_Repos" $silverLH "ado_silver"
Start-Sleep 2

# Deploy Gold notebooks
Write-Host "`n=== Deploying Gold Notebooks ==="
$nb_gold_facts = Deploy-Notebook "$repoRoot\notebooks\gold\01_build_facts.py" "07_Gold_Build_Facts" $goldLH "ado_gold"
Start-Sleep 2
$nb_gold_dims = Deploy-Notebook "$repoRoot\notebooks\gold\02_build_dimensions.py" "08_Gold_Build_Dimensions" $goldLH "ado_gold"

# Output notebook IDs for pipeline creation
Write-Host "`n=== Notebook IDs ==="
$notebookIds = @{
    "01_Bronze_Ingest_WorkItems" = $nb_bronze_wi
    "02_Bronze_Ingest_Boards" = $nb_bronze_boards
    "03_Bronze_Ingest_Repos" = $nb_bronze_repos
    "04_Silver_Transform_WorkItems" = $nb_silver_wi
    "05_Silver_Transform_Boards" = $nb_silver_boards
    "06_Silver_Transform_Repos" = $nb_silver_repos
    "07_Gold_Build_Facts" = $nb_gold_facts
    "08_Gold_Build_Dimensions" = $nb_gold_dims
}
$notebookIds | ConvertTo-Json | Out-File "C:\temp\notebook_ids.json" -Encoding utf8
$notebookIds | Format-Table -AutoSize
