# Module 6: Kafka Stream Processor (Structured Streaming)

class KafkaStreamProcessor:
    def __init__(self, config):
        self.config = config
    
    def process_sales_stream(self):
        return SparkSessionManager().get_spark() \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.bootstrap_servers) \
            .option("subscribe", self.config.sales_topic) \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json("json", self.sales_schema).alias("data")) \
            .select("data.*")
    
    @property
    def sales_schema(self):
        return StructType([
            StructField("transaction_id", StringType()),
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("timestamp", TimestampType())
        ])