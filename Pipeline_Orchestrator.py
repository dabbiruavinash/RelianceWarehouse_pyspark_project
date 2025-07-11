# Module 11: Pipeline Orchestrator

class PipelineOrchestrator:
    def __init__(self):
        self.config = PipelineConfig()
        self.spark = SparkSessionManager().get_spark()
        
        # Initialize all components
        self.oracle_reader = OracleReader(self.config.sources['oracle'])
        self.s3_reader = S3Reader(self.config.sources['s3'])
        self.kafka_processor = KafkaStreamProcessor(self.config.sources['kafka'])
        self.snowflake_connector = SnowflakeConnector(self.config.sources['snowflake'])
        self.transformer = DataTransformer()
        self.api_integration = APIIntegration(self.config.api)
        self.writer = AzureBlobWriter(self.config.target)
    
    def run_daily_pipeline(self):
        try:
            # Extract
            inventory = self.oracle_reader.read_inventory()
            products = self.s3_reader.read_product_catalog()
            customers = self.snowflake_connector.read_customer_data()
            
            # Transform
            enriched_inventory = self.transformer.join_inventory_with_products(inventory, products)
            sales = self.snowflake_connector.read_sales_history(30)
            inventory_turnover = self.transformer.calculate_inventory_turnover(enriched_inventory, sales)
            products_with_pricing = self.api_integration.enrich_products_with_pricing(products)
            
            # Load
            self.writer.write_daily_snapshot(inventory_turnover)
            self.writer.write_processed_data(products_with_pricing, "product_master")
            self.writer.write_processed_data(customers, "customer_master")
            
            return True
        except Exception as e:
            self.spark.sparkContext.setJobDescription(f"Failed: {str(e)}")
            raise

    def run_real_time_pipeline(self):
        sales_stream = self.kafka_processor.process_sales_stream()
        
        query = sales_stream.writeStream \
            .foreachBatch(self.process_real_time_batch) \
            .start()
        
        return query
    
    def process_real_time_batch(self, batch_df, batch_id):
        # Micro-batch processing
        current_inventory = self.oracle_reader.read_inventory()
        updated_inventory = self.transformer.adjust_inventory(current_inventory, batch_df)
        
        self.writer.write_processed_data(
            updated_inventory,
            "real_time/inventory",
            mode="overwrite"
        )