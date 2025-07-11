# Module 1: Configuration Manager (Python Classes)

class PipelineConfig:
    def __init__(self):
        self.sources = {
            'oracle': OracleConfig(),
            's3': S3Config(),
            'kafka': KafkaConfig(),
            'snowflake': SnowflakeConfig()
        }
        self.target = AzureBlobConfig()
    
    def validate(self):
        # Validate all configurations
        pass

class SourceConfig(ABC):
    @abstractmethod
    def get_reader(self, spark):
        pass

class OracleConfig(SourceConfig):
    def __init__(self):
        self.url = "jdbc:oracle:thin:@//host:port/service"
        self.user = "user"
        self.password = "password"
    
    def get_reader(self, spark):
        return spark.read.format("jdbc").options(
            url=self.url,
            user=self.user,
            password=self.password,
            driver="oracle.jdbc.OracleDriver"
        )