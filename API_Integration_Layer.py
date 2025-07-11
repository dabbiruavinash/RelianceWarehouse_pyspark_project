# Module 9: API Integration Layer

class APIIntegration:
    def __init__(self, config):
        self.config = config
    
    def get_external_pricing(self, product_ids):
        url = f"{self.config.base_url}/pricing"
        response = requests.post(
            url,
            json={"product_ids": product_ids},
            headers={"Authorization": f"Bearer {self.config.api_key}"}
        )
        return response.json()
    
    def enrich_products_with_pricing(self, products_df):
        product_ids = [row.product_id for row in products_df.select("product_id").distinct().collect()]
        pricing_data = self.get_external_pricing(product_ids)
        
        pricing_rdd = SparkSessionManager().get_spark().sparkContext.parallelize(
            [(k, v) for k, v in pricing_data.items()]
        )
        pricing_schema = StructType([
            StructField("product_id", StringType()),
            StructField("current_price", DecimalType(10,2))
        ])
        pricing_df = SparkSessionManager().get_spark().createDataFrame(pricing_rdd, pricing_schema)
        
        return products_df.join(pricing_df, "product_id", "left")