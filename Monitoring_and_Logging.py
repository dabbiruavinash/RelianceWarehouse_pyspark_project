# Module 12: Monitoring and Logging

class PipelineMonitor:
    def __init__(self):
        self.spark = SparkSessionManager().get_spark()
        self.logger = logging.getLogger("PipelineMonitor")
    
    def log_metrics(self, df, name):
        row_count = df.count()
        self.logger.info(f"DataFrame '{name}' has {row_count} rows")
        
        for col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                self.logger.warning(f"Column '{col}' has {null_count} null values")
        
        df.createOrReplaceTempView(f"metrics_{name}")
        self.spark.sql(f"CACHE TABLE metrics_{name}")
    
    def track_execution_time(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            
            duration = end_time - start_time
            self.logger.info(f"Execution of {func.__name__} took {duration:.2f} seconds")
            
            # Send to monitoring system
            self.spark.sparkContext.parallelize([{
                "event": "execution_time",
                "function": func.__name__,
                "duration": duration,
                "timestamp": datetime.now().isoformat()
            }]).toDF().write.saveAsTable("pipeline_execution_metrics")
            
            return result
        return wrapper