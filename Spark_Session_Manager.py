# Module 2: Spark Session Manager (Singleton Pattern)

class SparkSessionManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.spark = SparkSession.builder \
                .appName("RelianceWarehousePipeline") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.executor.memory", "8g") \
                .getOrCreate()
        return cls._instance
    
    def get_spark(self):
        return self.spark