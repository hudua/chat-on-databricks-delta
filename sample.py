import sys
import requests
import json
import os

# Configurations (replace with your actual values or set as environment variables)
AZURE_OPENAI_ENDPOINT = "https://.openai.azure.com/"
AZURE_OPENAI_API_KEY = ""
AZURE_OPENAI_DEPLOYMENT = "gpt-4o"
DATABRICKS_INSTANCE = "https://adb-.azuredatabricks.net"
DATABRICKS_TOKEN = ""
DATABRICKS_JOB_ID = ""  # The job should accept a parameter named "sql_query"

def get_sql_query(nl_query):
    url = f"{AZURE_OPENAI_ENDPOINT}openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version=2024-02-15-preview"
    headers = {
        "api-key": AZURE_OPENAI_API_KEY,
        "Content-Type": "application/json"
    }
    data = {
        "messages": [
            {"role": "system", "content": "You are an assistant that translates natural language to SQL queries. The sql table is called sample.data and has columns: id, name, age, height, weight, and gender."},
            {"role": "user", "content": f"Translate this to a SQL query: {nl_query}"}
        ],
        "max_tokens": 256,
        "temperature": 0
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    sql_query = response.json()["choices"][0]["message"]["content"].strip().replace("`", "").replace("sql", "").replace("\n"," ")
    return sql_query

def run_databricks_job(sql_query):
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "job_id": int(DATABRICKS_JOB_ID),
        "notebook_params": {
            "sql": sql_query
        }
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    run_id = response.json()["run_id"]

    # Poll for result
    result_url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}"
    while True:
        result_resp = requests.get(result_url, headers=headers)
        result_resp.raise_for_status()
        result_data = result_resp.json()
        if result_data["state"]["life_cycle_state"] in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
    task_id = result_data["tasks"][0]["run_id"]
    output_url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get-output?run_id={task_id}"
    output_resp = requests.get(output_url, headers=headers)
    output_data = output_resp.json()
    # Get output
    return output_data['notebook_output']['result']

if __name__ == "__main__":
    nl_query = sys.argv[1]
    sql_query = get_sql_query(nl_query)
    print("Generated SQL Query:")
    print(sql_query)
    result = run_databricks_job(sql_query)
    print("Databricks Job Result:")
    print(result)
