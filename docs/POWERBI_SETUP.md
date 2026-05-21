# Power BI Report Setup

## Semantic Model (DirectLake)

The Gold lakehouse (`ado_gold`) has a SQL endpoint that Power BI can connect to via DirectLake mode for zero-import, high-performance analytics.

### Connection Details

| Property | Value |
|----------|-------|
| Workspace | AzureDevOps_Analytics |
| Lakehouse | ado_gold |
| Lakehouse ID | `00000000-0000-0000-0000-000000000009` |
| Mode | DirectLake (automatic with Fabric Lakehouse) |

### Recommended Report Pages

#### Page 1: Sprint Performance
- **Velocity trend** — line chart of `completed_story_points` over sprints
- **Sprint completion rate** — bar chart comparing planned vs. delivered items
- **Active sprint board** — table showing current sprint items by state
- **Key metrics cards**: Current velocity, avg cycle time, completion %

#### Page 2: Backlog Health
- **Backlog by type** — stacked bar (Epic/Feature/Story/Bug/Task)
- **Aging analysis** — histogram of open item age
- **Unestimated items** — percentage gauge by work item type
- **Priority distribution** — donut chart

#### Page 3: Engineering Productivity
- **Commit frequency** — time series by week
- **PR cycle time trend** — line chart (avg days to merge)
- **PR throughput** — merged PRs per week
- **Active contributors** — distinct committers per week
- **Top contributors** — table with commit/PR counts

#### Page 4: Board Flow & WIP
- **Board column distribution** — stacked bar by project
- **WIP vs. limits** — comparison of actual vs. configured limits
- **Column dwell time** — avg days items spend in each column

### How to Create

1. Open Power BI Desktop or Fabric workspace
2. New Report → select `ado_gold` lakehouse as data source
3. Tables will appear automatically (DirectLake mode)
4. Build visuals using the fact/dimension tables above
5. Publish to the `AzureDevOps_Analytics` workspace

### Key Measures (DAX)

```dax
// Velocity (last N sprints)
Velocity = 
CALCULATE(
    SUM(fact_sprint_metrics[completed_story_points]),
    TOPN(1, ALL(dim_iteration), dim_iteration[start_date], DESC)
)

// Average Cycle Time
Avg Cycle Time = 
AVERAGE(fact_work_items[cycle_time_days])

// PR Merge Rate
PR Merge Rate = 
DIVIDE(
    COUNTROWS(FILTER(fact_pull_requests, fact_pull_requests[is_merged] = TRUE())),
    COUNTROWS(fact_pull_requests)
)

// Sprint Completion Rate
Sprint Completion % = 
DIVIDE(
    SUM(fact_sprint_metrics[completed_items]),
    SUM(fact_sprint_metrics[total_items])
)

// Backlog Growth (net new items per week)
Backlog Growth = 
COUNTROWS(FILTER(fact_work_items, fact_work_items[created_date] >= TODAY() - 7))
- COUNTROWS(FILTER(fact_work_items, fact_work_items[closed_date] >= TODAY() - 7))
```

### Relationships (Star Schema)

```
fact_work_items[iteration_path] → dim_iteration[iteration_path]
fact_work_items[project_name] → dim_project[project_name]
fact_work_items[assigned_to_email] → dim_team_member[member_email]
fact_sprint_metrics[iteration_id] → dim_iteration[iteration_id]
fact_commits[repo_id] → dim_repository[repo_id]
fact_pull_requests[repo_id] → dim_repository[repo_id]
fact_*[created_date/author_date] → dim_date[date_key]
```
