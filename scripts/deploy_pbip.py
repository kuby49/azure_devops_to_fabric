"""Deploy PBIP semantic model and report to Fabric workspace via REST API."""
import base64
import json
import os
import subprocess
import time

WORKSPACE_ID = "00000000-0000-0000-0000-000000000001"
AZ_CMD = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
BASE_URL = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
SM_DIR = r"C:\Users\<user>\azure_devops_to_fabric\reports\ADO_Analytics.SemanticModel"
REPORT_DIR = r"C:\Users\<user>\azure_devops_to_fabric\reports\ADO_Analytics.Report"


def get_token():
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com", "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True
    )
    return result.stdout.strip()


def az_rest(method, url, body=None):
    cmd = ["az", "rest", "--method", method, "--resource", "https://api.fabric.microsoft.com", "--url", url]
    if body:
        tmp = os.path.join(os.environ["TEMP"], "fabric_body.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(body, f)
        cmd += ["--body", f"@{tmp}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr}")
        return None
    if result.stdout.strip():
        return json.loads(result.stdout)
    return {}


def poll_lro(url, headers=None):
    """Poll long-running operation until complete."""
    for _ in range(60):
        time.sleep(3)
        result = subprocess.run(
            ["az", "rest", "--method", "get", "--resource", "https://api.fabric.microsoft.com", "--url", url],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            if "Succeeded" in result.stderr or "200" in result.stderr:
                return {"status": "Succeeded"}
            continue
        if result.stdout.strip():
            data = json.loads(result.stdout)
            status = data.get("status", "")
            if status in ("Succeeded", "Failed", "Cancelled"):
                return data
    return {"status": "Timeout"}


def encode_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    return base64.b64encode(content.encode("utf-8")).decode("ascii")


def collect_definition_parts(base_dir, prefix="definition"):
    """Collect all definition files as base64-encoded parts."""
    parts = []
    def_dir = os.path.join(base_dir, "definition")
    
    for root, dirs, files in os.walk(def_dir):
        for fname in files:
            filepath = os.path.join(root, fname)
            rel_path = os.path.relpath(filepath, base_dir).replace("\\", "/")
            parts.append({
                "path": rel_path,
                "payload": encode_file(filepath),
                "payloadType": "InlineBase64"
            })
    return parts


def create_semantic_model():
    print("=== Creating Semantic Model ===")
    parts = collect_definition_parts(SM_DIR)
    
    # Also include definition.pbism as a part
    pbism_path = os.path.join(SM_DIR, "definition.pbism")
    if os.path.exists(pbism_path):
        parts.append({
            "path": "definition.pbism",
            "payload": encode_file(pbism_path),
            "payloadType": "InlineBase64"
        })
    
    body = {
        "displayName": "ADO_Analytics",
        "type": "SemanticModel",
        "definition": {
            "parts": parts
        }
    }
    
    print(f"  Uploading {len(parts)} definition parts...")
    
    # Use az rest with the body
    tmp = os.path.join(os.environ["TEMP"], "fabric_sm_body.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(body, f)
    
    result = subprocess.run(
        ["az", "rest", "--method", "post", "--resource", "https://api.fabric.microsoft.com",
         "--url", f"{BASE_URL}/items", "--body", f"@{tmp}"],
        capture_output=True, text=True
    )
    
    if result.returncode != 0:
        err = result.stderr
        # Check for LRO (202 Accepted)
        if "location" in err.lower() or "202" in err:
            print("  LRO initiated, polling...")
            # Extract operation URL from headers - parse from error
            # Fall back to checking items list
            time.sleep(5)
            items = az_rest("get", f"{BASE_URL}/semanticModels")
            if items and items.get("value"):
                for item in items["value"]:
                    if item["displayName"] == "ADO_Analytics":
                        print(f"  ✓ Semantic Model created: {item['id']}")
                        return item["id"]
        print(f"  Error: {err[:500]}")
        return None
    
    if result.stdout.strip():
        data = json.loads(result.stdout)
        sm_id = data.get("id")
        print(f"  ✓ Semantic Model created: {sm_id}")
        return sm_id
    
    # Check if it was created (sometimes 201 with no body)
    time.sleep(3)
    items = az_rest("get", f"{BASE_URL}/semanticModels")
    if items and items.get("value"):
        for item in items["value"]:
            if item["displayName"] == "ADO_Analytics":
                print(f"  ✓ Semantic Model created: {item['id']}")
                return item["id"]
    
    print("  Failed to create semantic model")
    return None


def create_report(semantic_model_id):
    print("\n=== Creating Report ===")
    parts = collect_definition_parts(REPORT_DIR)
    
    # Update definition.pbir to reference the deployed semantic model by connection
    pbir_content = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": semantic_model_id,
                "name": "EntityDataSource",
                "connectionType": "pbiServiceXmlaStyleLive"
            }
        }
    }
    pbir_b64 = base64.b64encode(json.dumps(pbir_content, indent=2).encode("utf-8")).decode("ascii")
    parts.append({
        "path": "definition.pbir",
        "payload": pbir_b64,
        "payloadType": "InlineBase64"
    })
    
    body = {
        "displayName": "ADO_Analytics",
        "type": "Report",
        "definition": {
            "parts": parts
        }
    }
    
    print(f"  Uploading {len(parts)} definition parts...")
    
    tmp = os.path.join(os.environ["TEMP"], "fabric_rpt_body.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(body, f)
    
    result = subprocess.run(
        ["az", "rest", "--method", "post", "--resource", "https://api.fabric.microsoft.com",
         "--url", f"{BASE_URL}/items", "--body", f"@{tmp}"],
        capture_output=True, text=True
    )
    
    if result.returncode != 0:
        err = result.stderr
        if "location" in err.lower() or "202" in err:
            print("  LRO initiated, waiting...")
            time.sleep(10)
            items = az_rest("get", f"{BASE_URL}/reports")
            if items and items.get("value"):
                for item in items["value"]:
                    if item["displayName"] == "ADO_Analytics":
                        print(f"  ✓ Report created: {item['id']}")
                        return item["id"]
        print(f"  Error: {err[:500]}")
        return None
    
    if result.stdout.strip():
        data = json.loads(result.stdout)
        print(f"  ✓ Report created: {data.get('id')}")
        return data.get("id")
    
    time.sleep(3)
    items = az_rest("get", f"{BASE_URL}/reports")
    if items and items.get("value"):
        for item in items["value"]:
            if item["displayName"] == "ADO_Analytics":
                print(f"  ✓ Report created: {item['id']}")
                return item["id"]
    
    print("  Failed to create report")
    return None


if __name__ == "__main__":
    os.environ["AZURE_CONFIG_DIR"] = os.path.expanduser("~\\.azure-mcap")
    
    sm_id = create_semantic_model()
    if sm_id:
        report_id = create_report(sm_id)
        if report_id:
            print(f"\n✅ Deployment complete!")
            print(f"   Semantic Model: {sm_id}")
            print(f"   Report: {report_id}")
            print(f"   Workspace: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}")
        else:
            print("\n⚠️ Semantic model deployed but report creation failed")
    else:
        print("\n❌ Deployment failed")
