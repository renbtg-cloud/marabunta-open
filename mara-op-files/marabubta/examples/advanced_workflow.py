# Marabunta - Licensed under the MIT License.
import json
from marabunta_sdk.workflow import workflow, task, If, ForEach, Parallel, Condition

@task(retries=3)
def analyze_dataset(source: str):
    print(f"Analyzing {source}")
    return {"drift_detected": True, "files": ["chunk1.csv", "chunk2.csv", "chunk3.csv"]}

@task(memory_min=4096)
def alert_human(msg: str):
    print(f"ALERT: {msg}")
    return {}

@task(timeout_secs=600)
def retrain_model(file_chunk: str):
    print(f"Retraining on {file_chunk}")
    return {"model_loss": 0.05}

@task()
def evaluate_model():
    print("Evaluating new model")
    return {"accuracy": 0.98}

@task()
def deploy_model():
    print("Deploying model to prod")
    return {"status": "deployed"}

@task()
def run_backup():
    print("Backing up data")
    return {"status": "backed_up"}

@workflow(name="adaptive_ml_pipeline")
def ml_pipeline(dataset_url: str):
    # 1. Analyze
    analysis = analyze_dataset(dataset_url)
    
    # 2. Branch conditionally based on analysis result
    drift_cond = If(Condition.on_output_match(analysis, "drift_detected", True))
    with drift_cond:
        
        # 3. Parallel block (Alert human while retraining starts)
        with Parallel():
            alert_human("Data drift detected! Retraining initiated.")
            
            # 4. ForEach Loop: Dynamically spray chunks across the swarm
            with ForEach(items=analysis, item_var="chunk"):
                retrain_model("${chunk}")
        
        # 5. Evaluate after retraining loop completes
        eval_res = evaluate_model()
        
        # 6. Nested If
        with If(Condition.on_output_match(eval_res, "accuracy", 0.95, "greater_than")):
            deploy_model()
            
    with drift_cond.Else():
        # If no drift, just do a daily backup
        run_backup()

if __name__ == "__main__":
    dag = ml_pipeline("s3://data/latest")
    print(json.dumps(dag, indent=2))
