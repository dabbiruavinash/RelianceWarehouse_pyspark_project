# Module 15: Performance Optimizer

class PerformanceOptimizer:
    def __init__(self):
        self.spark = SparkSessionManager().get_spark()
    
    def cache_frequently_used_tables(self):
        tables = ["products", "customers", "inventory_snapshot"]
        for table in tables:
            self.spark.sql(f"CACHE TABLE {table}")
    
    def optimize_join_strategy(self, df1, df2, join_columns):
        # Broadcast smaller table if possible
        size_df1 = df1.rdd.count()
        size_df2 = df2.rdd.count()
        
        if size_df1 < size_df2 and size_df1 < 10_000_000:  # 10MB threshold
            return df1.join(broadcast(df2), join_columns)
        else:
            return df1.join(df2, join_columns)
    
    def apply_zordering(self, df, zorder_columns):
        # Note: This requires Delta Lake format
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.dataSkippingNumIndexedCols", len(zorder_columns)) \
            .saveAsTable("zordered_table")