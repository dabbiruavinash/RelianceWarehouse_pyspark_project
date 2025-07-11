# Module 3: Data Quality Framework (Decorators)

def validate_schema(expected_schema):
    def decorator(func):
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            actual_schema = set(df.columns)
            if not expected_schema.issubset(actual_schema):
                missing = expected_schema - actual_schema
                raise ValueError(f"Missing columns: {missing}")
            return df
        return wrapper
    return decorator

def check_null_percentage(threshold=0.3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                total_count = df.count()
                if total_count > 0 and (null_count / total_count) > threshold:
                    raise ValueError(f"Column {col} has {null_count/total_count:.2%} nulls (threshold: {threshold})")
            return df
        return wrapper
    return decorator