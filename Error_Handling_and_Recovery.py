# Module 13: Error Handling and Recovery

class ErrorHandler:
    def __init__(self):
        self.spark = SparkSessionManager().get_spark()
    
    def handle_error(self, error, context=None):
        error_data = {
            "error_message": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.now().isoformat(),
            "context": str(context)
        }
        
        # Write error to error log table
        error_df = self.spark.createDataFrame([error_data])
        error_df.write.mode("append").saveAsTable("pipeline_error_log")
        
        # Optionally send alert
        if isinstance(error, (ValueError, RuntimeError)):
            self.send_alert(error_data)
    
    def send_alert(self, error_data):
        # Implementation for sending alerts (email, Slack, etc.)
        pass
    
    def retry_operation(self, func, max_retries=3, delay=5):
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    self.handle_error(e, f"Attempt {attempt + 1} failed")
                    time.sleep(delay)
            raise last_error
        return wrapper