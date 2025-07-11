# Module 14: Data Partitioning Strategy

class DataPartitioner:
    @staticmethod
    def partition_by_date(df, date_column):
        return df.withColumn(
            "year", year(col(date_column))
        ).withColumn(
            "month", month(col(date_column))
        ).withColumn(
            "day", dayofmonth(col(date_column))
        )
    
    @staticmethod
    def optimize_write_partitioning(df, partition_columns):
        # Determine optimal number of partitions
        approx_size = df.rdd.flatMap(lambda x: x).map(lambda x: len(str(x))).sum()
        target_partition_size = 128 * 1024 * 1024  # 128MB
        num_partitions = max(1, int(approx_size / target_partition_size))
        
        return df.repartition(num_partitions, *partition_columns)
    
    @staticmethod
    def apply_bucketeting(df, num_buckets, bucket_columns):
        return df.write \
            .bucketBy(num_buckets, *bucket_columns) \
            .sortBy(*bucket_columns) \
            .saveAsTable("bucketed_table")