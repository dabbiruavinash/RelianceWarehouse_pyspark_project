# Module 4: Oracle Reader (Spark JDBC)

class OracleReader:
    def __init__(self, config):
        self.config = config
    
    @validate_schema({"product_id", "quantity", "timestamp"})
    @check_null_percentage(0.1)
    def read_inventory(self):
        return self.config.get_reader() \
            .option("dbtable", "inventory") \
            .load()
    
    def read_transactions(self):
        return self.config.get_reader() \
            .option("dbtable", "(SELECT * FROM transactions WHERE date > SYSDATE-30)") \
            .load()