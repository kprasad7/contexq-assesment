#!/usr/bin/env python3
"""
View ML Training Metrics from AWS Glue Job Logs
"""
import boto3
import json
import re
from datetime import datetime
from tabulate import tabulate

def get_ml_job_runs():
    """Get successful ML training job runs"""
    glue = boto3.client('glue')
    
    response = glue.get_job_runs(
        JobName='contexq-dev-ml-training',
        MaxResults=10
    )
    
    succeeded_runs = [
        run for run in response['JobRuns'] 
        if run['JobRunState'] == 'SUCCEEDED'
    ]
    
    return succeeded_runs

def extract_metrics_from_logs(job_run_id):
    """Extract metrics from CloudWatch logs"""
    logs = boto3.client('logs')
    
    try:
        response = logs.get_log_events(
            logGroupName='/aws-glue/jobs/output',
            logStreamName=job_run_id,
            startFromHead=True
        )
        
        metrics = {
            'job_run_id': job_run_id,
            'timestamp': None,
            'auc': None,
            'f1_score': None,
            'total_records': None,
            'high_profit': None,
            'low_profit': None,
            'mlflow_run_id': None
        }
        
        for event in response['events']:
            message = event['message']
            
            # Extract timestamp
            if not metrics['timestamp']:
                metrics['timestamp'] = datetime.fromtimestamp(
                    event['timestamp'] / 1000
                ).strftime('%Y-%m-%d %H:%M:%S')
            
            # Extract AUC
            if 'AUC:' in message:
                match = re.search(r'AUC:\s+([\d.]+)', message)
                if match:
                    metrics['auc'] = float(match.group(1))
            
            # Extract F1-Score
            if 'F1-Score:' in message:
                match = re.search(r'F1-Score:\s+([\d.]+)', message)
                if match:
                    metrics['f1_score'] = float(match.group(1))
            
            # Extract record counts
            if 'total_records' in message and 'high_profit_count' in message:
                # Parse the table output
                match = re.search(r'\|(\d+)\s+\|(\d+)\s+\|(\d+)', message)
                if match:
                    metrics['total_records'] = int(match.group(1))
                    metrics['high_profit'] = int(match.group(2))
                    metrics['low_profit'] = int(match.group(3))
            
            # Extract MLflow run ID
            if 'MLflow run ID:' in message or 'MLflow Run ID:' in message:
                match = re.search(r'[Rr]un ID:\s+([a-f0-9]+)', message)
                if match:
                    metrics['mlflow_run_id'] = match.group(1)
        
        return metrics
    
    except Exception as e:
        print(f"Error reading logs for {job_run_id}: {e}")
        return None

def main():
    print("=" * 80)
    print("ML Training Metrics Dashboard")
    print("=" * 80)
    print()
    
    # Get job runs
    print("Fetching ML training job runs...")
    job_runs = get_ml_job_runs()
    
    if not job_runs:
        print("No successful job runs found.")
        return
    
    print(f"Found {len(job_runs)} successful runs\n")
    
    # Extract metrics from each run
    all_metrics = []
    for run in job_runs:
        job_run_id = run['Id']
        started_on = run['StartedOn'].strftime('%Y-%m-%d %H:%M') if isinstance(run['StartedOn'], datetime) else datetime.fromtimestamp(run['StartedOn']).strftime('%Y-%m-%d %H:%M')
        execution_time = run.get('ExecutionTime', 0)
        
        print(f"Processing {job_run_id[:20]}... ", end='', flush=True)
        metrics = extract_metrics_from_logs(job_run_id)
        
        if metrics and metrics['auc']:
            metrics['started_on'] = started_on
            metrics['execution_time'] = f"{execution_time}s"
            all_metrics.append(metrics)
            print("✓")
        else:
            print("✗ (no metrics)")
    
    if not all_metrics:
        print("\nNo metrics found in logs.")
        return
    
    # Display results
    print("\n" + "=" * 80)
    print("TRAINING RESULTS")
    print("=" * 80 + "\n")
    
    # Summary table
    table_data = []
    for m in all_metrics:
        table_data.append([
            m['started_on'],
            f"{m['auc']:.4f}" if m['auc'] else 'N/A',
            f"{m['f1_score']:.4f}" if m['f1_score'] else 'N/A',
            m['total_records'] or 'N/A',
            f"{m['high_profit']}/{m['low_profit']}" if m['high_profit'] else 'N/A',
            m['execution_time'],
            m['mlflow_run_id'][:12] if m['mlflow_run_id'] else 'N/A'
        ])
    
    headers = ['Timestamp', 'AUC', 'F1-Score', 'Records', 'High/Low', 'Time', 'MLflow Run']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # Latest run details
    if all_metrics:
        latest = all_metrics[0]
        print("\n" + "=" * 80)
        print("LATEST RUN DETAILS")
        print("=" * 80)
        print(f"Timestamp:       {latest['started_on']}")
        print(f"AUC Score:       {latest['auc']:.4f} (Area Under ROC Curve)")
        print(f"F1 Score:        {latest['f1_score']:.4f} (Harmonic mean of precision/recall)")
        print(f"Total Records:   {latest['total_records']:,}")
        print(f"High Profit:     {latest['high_profit']:,} ({latest['high_profit']/latest['total_records']*100:.1f}%)")
        print(f"Low Profit:      {latest['low_profit']:,} ({latest['low_profit']/latest['total_records']*100:.1f}%)")
        print(f"Execution Time:  {latest['execution_time']}")
        print(f"MLflow Run ID:   {latest['mlflow_run_id']}")
        print()
    
    print("=" * 80)
    print("To view in MLflow UI, run:")
    print("  ./scripts/setup_mlflow_server.sh")
    print("=" * 80)

if __name__ == '__main__':
    main()
