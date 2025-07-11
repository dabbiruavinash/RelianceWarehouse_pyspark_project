# Module 16: Data Encryption Module


class DataEncryptor:
    def __init__(self, config):
        self.config = config
    
    def encrypt_column(self, df, column_name):
        if column_name not in df.columns:
            return df
        
        return df.withColumn(
            column_name,
            expr(f"aes_encrypt({column_name}, '{self.config.encryption_key}', 'GCM')")
        )
    
    def decrypt_column(self, df, column_name):
        if column_name not in df.columns:
            return df
        
        return df.withColumn(
            column_name,
            expr(f"aes_decrypt({column_name}, '{self.config.encryption_key}', 'GCM')")
        )
    
    def mask_pii(self, df, columns):
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(length(col(col_name)) > 3,
                         concat(
                             substr(col(col_name), 1, 2),
                             lit("*****"),
                             substr(col(col_name), -1, 1)
                         )
                    ).otherwise(lit("****"))
                )
        return df
