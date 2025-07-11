# Module 8: Data Transformation Engine

class DataTransformer:
    @staticmethod
    def join_inventory_with_products(inventory_df, products_df):
        return inventory_df.join(
            products_df,
            inventory_df.product_id == products_df.id,
            "left"
        ).drop(products_df.id)
    
    @staticmethod
    def calculate_inventory_turnover(inventory_df, sales_df):
        sales_agg = sales_df.groupBy("product_id").agg(
            sum("quantity").alias("total_sales")
        )
        return inventory_df.join(
            sales_agg,
            inventory_df.product_id == sales_agg.product_id,
            "left"
        ).withColumn(
            "turnover_ratio",
            col("total_sales") / col("quantity")
        )
    
    @staticmethod
    def normalize_warehouse_data(warehouse_df):
        return warehouse_df.withColumn(
            "normalized_capacity",
            col("capacity") / max("capacity").over(Window.partitionBy())
        )