# Module 19: CI/CD Integration

class PipelineDeployer:
    def __init__(self, config):
        self.config = config
    
    def deploy_to_production(self):
        # Validate all configurations
        self.validate_configurations()
        
        # Run integration tests
        self.run_integration_tests()
        
        # Package the application
        self.create_deployment_package()
        
        # Deploy to production cluster
        self.submit_spark_job()
    
    def validate_configurations(self):
        # Check all required configurations are present
        required_configs = [
            'spark.master', 'spark.executor.memory',
            'oracle.url', 's3.bucket', 'kafka.bootstrap_servers',
            'snowflake.account', 'azure.blob.account_name'
        ]
        
        missing = [cfg for cfg in required_configs if cfg not in self.config]
        if missing:
            raise ValueError(f"Missing required configurations: {missing}")
    
    def run_integration_tests(self):
        # Implement integration tests that verify connectivity to all systems
        test_results = {
            'oracle': self.test_oracle_connection(),
            's3': self.test_s3_connection(),
            'kafka': self.test_kafka_connection(),
            'snowflake': self.test_snowflake_connection(),
            'azure': self.test_azure_connection()
        }
        
        failures = {k: v for k, v in test_results.items() if not v}
        if failures:
            raise RuntimeError(f"Integration test failures: {failures}")
    
    def create_deployment_package(self):
        # Create a deployment package with all dependencies
        subprocess.run([
            "pip", "install", "-r", "requirements.txt", 
            "--target", "deployment_package"
        ], check=True)
        
        # Package the application code
        subprocess.run([
            "zip", "-r", "pipeline_deployment.zip", 
            "deployment_package", "src"
        ], check=True)
    
    def submit_spark_job(self):
        # Submit the Spark job to the cluster
        spark_submit_cmd = [
            "spark-submit",
            "--master", self.config['spark.master'],
            "--executor-memory", self.config['spark.executor.memory'],
            "--py-files", "pipeline_deployment.zip",
            "src/main.py"
        ]
        
        subprocess.run(spark_submit_cmd, check=True)