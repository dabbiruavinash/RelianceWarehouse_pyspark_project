# Module 17: Schema Evolution Handler

class SchemaManager:
    def __init__(self):
        self.spark = SparkSessionManager().get_spark()
    
    def handle_schema_changes(self, df, expected_schema):
        current_columns = set(df.columns)
        expected_columns = set(expected_schema.fieldNames())
        
        # Add missing columns with null values
        for col_name in expected_columns - current_columns:
            df = df.withColumn(col_name, lit(None).cast(expected_schema[col_name].dataType))
        
        # Remove extra columns
        for col_name in current_columns - expected_columns:
            df = df.drop(col_name)
        
        # Ensure correct data types
        for field in expected_schema:
            if field.name in df.columns:
                current_type = df.schema[field.name].dataType
                if current_type != field.dataType:
                    df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        
        return df
    
    def merge_schemas(self, df1, df2):
        schema1 = df1.schema
        schema2 = df2.schema
        
        # Create union of fields
        merged_fields = {}
        for field in schema1.fields + schema2.fields:
            if field.name not in merged_fields:
                merged_fields[field.name] = field
            else:
                # Resolve type conflicts by choosing the more general type
                existing_type = merged_fields[field.name].dataType
                if existing_type != field.dataType:
                    merged_fields[field.name].dataType = self._resolve_type_conflict(existing_type, field.dataType)
        
        return StructType(list(merged_fields.values()))
    
    def _resolve_type_conflict(self, type1, type2):
        # Simple type resolution - in production you'd need more sophisticated logic
        type_hierarchy = [NullType, ByteType, ShortType, IntegerType, LongType, 
                         FloatType, DoubleType, DecimalType, StringType]
        
        idx1 = next((i for i, t in enumerate(type_hierarchy) if isinstance(type1, t)), len(type_hierarchy))
        idx2 = next((i for i, t in enumerate(type_hierarchy) if isinstance(type2, t)), len(type_hierarchy))
        
        return type_hierarchy[max(idx1, idx2)]()

