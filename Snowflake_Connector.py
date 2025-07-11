# Snowflake Connector (Spark Snowflake)

class SnowflakeConnector:
    def __init__(self, config):
        self.config = config
    
    def read_customer_data(self):
        return SparkSessionManager().get_spark() \
            .read \
            .format("snowflake") \
            .options(**self.config.connection_options) \
            .option("query", "SELECT * FROM customer_data WHERE is_active = TRUE") \
            .load()
    
    def read_sales_history(self, days=365):
        return SparkSessionManager().get_spark() \
            .read \
            .format("snowflake") \
            .options(**self.config.connection_options) \
            .option("query", f"""
                SELECT * FROM sales_history 
                WHERE transaction_date >= DATEADD(day, -{days}, CURRENT_DATE())
            """) \
            .load()