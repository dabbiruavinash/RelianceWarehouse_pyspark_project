# Module 18: API Service (FastAPI)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000

@app.post("/query")
def execute_query(request: QueryRequest):
    try:
        spark = SparkSessionManager().get_spark()
        df = spark.sql(request.sql)
        
        if request.limit > 0:
            df = df.limit(request.limit)
        
        return {
            "columns": df.columns,
            "data": df.collect(),
            "count": df.count()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/metrics")
def get_pipeline_metrics():
    spark = SparkSessionManager().get_spark()
    metrics_df = spark.sql("""
        SELECT function, AVG(duration) as avg_duration, COUNT(*) as executions
        FROM pipeline_execution_metrics
        GROUP BY function
        ORDER BY avg_duration DESC
    """)
    return metrics_df.collect()

@app.get("/errors")
def get_recent_errors(limit: int = 10):
    spark = SparkSessionManager().get_spark()
    errors_df = spark.sql(f"""
        SELECT * FROM pipeline_error_log
        ORDER BY timestamp DESC
        LIMIT {limit}
    """)
    return errors_df.collect()