from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Dict, Any

app = FastAPI(title="BigQuery API", description="BigQuery Cost API")

credentials = service_account.Credentials.from_service_account_file(
    "meyra-service-account-key.json"
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

COST_QUERY = """
SELECT
SUM((total_bytes_billed/POW(1024, 4)) * 7.00) AS estimated_cost_usd
FROM `region-europe-west3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time BETWEEN TIMESTAMP(DATETIME(CURRENT_DATE("Europe/Istanbul"), TIME '07:00:00'), "Europe/Istanbul")
AND  TIMESTAMP(DATETIME(DATE_ADD(CURRENT_DATE("Europe/Istanbul"), INTERVAL 1 DAY), TIME '07:00:00'), "Europe/Istanbul")
"""

@app.get("/api/getCost", response_model=Dict[str, Any])
async def get_cost():
  
    try:
        query_job = client.query(COST_QUERY, timeout=60)
        results = query_job.result()
        rows = []
        for row in results:
            rows.append({key: value for key, value in row.items()})
        
        return {"success": True, "data": rows, "row_count": len(rows)}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Server is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 