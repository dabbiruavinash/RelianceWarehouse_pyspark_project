# Module 10: Azure Blob Writer

class AzureBlobWriter:
    def __init__(self, config):
        self.config = config
    
    def write_processed_data(self, df, path, mode="overwrite"):
        df.write \
            .mode(mode) \
            .parquet(f"wasbs://{self.config.container}@{self.config.account_name}.blob.core.windows.net/{path}")
    
    def write_daily_snapshot(self, df):
        current_date = datetime.now().strftime("%Y-%m-%d")
        self.write_processed_data(
            df,
            f"daily_snapshots/{current_date}/inventory.parquet"
        )