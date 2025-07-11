# Module 5: S3 Reader (Spark S3)

class S3Reader:
    def __init__(self, config):
        self.config = config
    
    def read_product_catalog(self):
        return SparkSessionManager().get_spark() \
            .read.parquet(f"s3a://{self.config.bucket}/product_catalog/")
    
    def read_warehouse_layout(self):
        return SparkSessionManager().get_spark() \
            .read.json(f"s3a://{self.config.bucket}/warehouse_layout/")