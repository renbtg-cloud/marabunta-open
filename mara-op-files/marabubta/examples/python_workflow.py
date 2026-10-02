# Marabunta - Licensed under the MIT License.
from marabunta_sdk.workflow import workflow, task

@task(runtime="python3", memory_min=1024, retries=3)
def fetch_data(source_url: str):
    import urllib.request
    print(f"Fetching data from {source_url}...")
    # Mock data fetch
    return {"raw_data": [1, 2, 3, 4, 5], "status": "ok"}

@task(runtime="python3", timeout_secs=300)
def process_data(data: dict, multiplier: int):
    print(f"Processing data: {data} with multiplier {multiplier}")
    processed = [x * multiplier for x in data.get("raw_data", [])]
    return {"processed_data": processed}

@task(runtime="python3")
def finalize_report(processed_a: dict, processed_b: dict):
    print("Combining results...")
    return {
        "final_report": "SUCCESS",
        "dataset_a": processed_a.get("processed_data"),
        "dataset_b": processed_b.get("processed_data"),
        "total_sum": sum(processed_a.get("processed_data", [])) + sum(processed_b.get("processed_data", []))
    }

@workflow(name="monthly_financial_report", version="1.2.0")
def build_report_dag(source_url: str, base_multiplier: int):
    # Step 1: Fetch
    raw_data = fetch_data(source_url)
    
    # Step 2: Branch into parallel processing
    branch_a = process_data(raw_data, multiplier=base_multiplier)
    branch_b = process_data(raw_data, multiplier=base_multiplier * 2)
    
    # Step 3: Combine dependencies
    final = finalize_report(branch_a, branch_b)
    return final

if __name__ == "__main__":
    import json
    # Generating the JSON payload expected by the Marabunta Coordinator
    dag_json = build_report_dag("https://api.finance.local/data", 10)
    print(json.dumps(dag_json, indent=2))
