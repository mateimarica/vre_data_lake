from enum import Enum, auto

class Filetype(Enum):
    APACHE_AVRO = auto()
    APACHE_ORC = auto()
    APACHE_PARQUET = auto()
    JSON = auto()
    BINARY_JSON = auto()
    XML = auto()
    AMAZON_ION = auto()
    COMBINED_APACHE_LOG = auto()
    APACHE_LOG = auto()
    LINUX_KERNEL_LOG = auto()
    MICROSOFT_LOG = auto()
    RUBY_LOG = auto()
    SQUID_3_LOG = auto()
    REDIS_MONITOR_LOG = auto()
    REDIS_LOG = auto()
    CSV = auto()
    AMAZON_REDSHIFT = auto()
    MYSQL = auto()
    POSTGRESQL = auto()
    ORACLE_DATABASE = auto()
    MICROSOFT_SQL_SERVER = auto()
    AMAZON_DYNAMODB = auto()
    OTHER = auto()
    
    def glue_classifer(self):
        return {
            Filetype.APACHE_AVRO: "avro",
            Filetype.APACHE_ORC: "orc",
            Filetype.APACHE_PARQUET: "parquet",
            Filetype.JSON: "json",
            Filetype.BINARY_JSON: "bson",
            Filetype.XML: "xml",
            Filetype.AMAZON_ION: "ion",
            Filetype.COMBINED_APACHE_LOG: "combined_apache",
            Filetype.APACHE_LOG: "apache",
            Filetype.LINUX_KERNEL_LOG: "linux_kernel",
            Filetype.MICROSOFT_LOG: "microsoft_log",
            Filetype.RUBY_LOG: "ruby_logger",
            Filetype.SQUID_3_LOG: "squid",
            Filetype.REDIS_MONITOR_LOG: "redismonlog",
            Filetype.REDIS_LOG: "redislog",
            Filetype.CSV: "csv",
            Filetype.AMAZON_REDSHIFT: "redshift",
            Filetype.MYSQL: "mysql",
            Filetype.POSTGRESQL: "postgresql",
            Filetype.ORACLE_DATABASE: "oracle",
            Filetype.MICROSOFT_SQL_SERVER: "sqlserver",
            Filetype.AMAZON_DYNAMODB: "dynamicdb",
            Filetype.OTHER: "UNKNOWN",
        }[self]
