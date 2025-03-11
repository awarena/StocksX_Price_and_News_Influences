import os
import sys

from typing import Dict, Any, List, Optional
from stocksx.data_pipeline.sub_modules.schema import DataSchema
from stocksx.data_pipeline.sub_modules.spark_manager import SparkManager
from stocksx.data_pipeline.sub_modules.logger import Logger
from stocksx.configs.processing_config import ProcessingConfig


class IcebergManager:
    """Manager for Apache Iceberg tables and operations."""
    
    def __init__(self, 
                 spark_manager: SparkManager, 
                 schema: DataSchema, 
                 config: ProcessingConfig):
        """Initialize IcebergManager.
        
        Args:
            spark_manager: SparkManager instance
            schema: DataSchema instance
            config: ProcessingConfig instance
        """
        self.spark = spark_manager
        self.schema = schema
        self.config = config
        self.catalog = getattr(spark_manager.config, 'iceberg_catalog', 'local')
        self.logger = Logger(config)
        
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get Iceberg table configuration.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table configuration
        """
        configs = {
            "stock_prices": {
                "partition_by": ["years(trade_date)", "symbol"],
                "comment": "Stock price historical data",
                "properties": {
                    "write.format.default": "parquet",
                    "write.metadata.compression-codec": "gzip"
                }
            }
        }
        
        return configs.get(table_name, {
            "partition_by": [],
            "comment": f"{table_name} table",
            "properties": {"write.format.default": "parquet"}
        })
    
    def generate_create_sql(self, table_name: str) -> str:
        """Generate SQL for creating an Iceberg table.
        
        Args:
            table_name: Name of the table to create
            
        Returns:
            SQL statement for creating the table
        """
        try:
            # Get schema for the requested table
            schema = self.schema.get_schema(table_name)
            
            # Get table configuration
            table_config = self.get_table_config(table_name)
            
            # Start building SQL
            sql_parts = [f"CREATE TABLE IF NOT EXISTS {self.catalog}.default.{table_name} ("]
            
            # Add columns
            columns = []
            for field in schema.fields:
                # Map Spark types to SQL types
                type_mapping = {
                    "StringType": "STRING",
                    "DateType": "DATE",
                    "FloatType": "DOUBLE",
                    "LongType": "BIGINT",
                    "IntegerType": "INT",
                    "BooleanType": "BOOLEAN",
                    "TimestampType": "TIMESTAMP"
                }
                
                spark_type = field.dataType.__class__.__name__
                sql_type = type_mapping.get(spark_type, "STRING")
                nullable = "" if field.nullable else "NOT NULL"
                
                columns.append(f"    {field.name} {sql_type} {nullable}")
            
            sql_parts.append(",\n".join(columns))
            sql_parts.append(")")
            
            # Add USING clause
            sql_parts.append("USING iceberg")
            
            # Add partitioning if defined
            if table_config.get("partition_by"):
                sql_parts.append(f"PARTITIONED BY ({', '.join(table_config['partition_by'])})")
            
            # Add comment if defined
            if table_config.get("comment"):
                sql_parts.append(f"COMMENT '{table_config['comment']}'")
            
            # Add properties if defined
            if table_config.get("properties"):
                sql_parts.append("TBLPROPERTIES (")
                props = [f"    '{k}' = '{v}'" for k, v in table_config["properties"].items()]
                sql_parts.append(",\n".join(props))
                sql_parts.append(")")
            
            return "\n".join(sql_parts)
            
        except ValueError as e:
            self.logger.error(f"Error generating Iceberg SQL: {str(e)}")
            return ""
    
    def initialize_tables(self, tables_to_create: Optional[List[str]] = None):
        """Initialize Iceberg tables.
        
        Args:
            tables_to_create: List of tables to create, or None for default tables
        """
        # Default tables to create
        if tables_to_create is None:
            tables_to_create = ["stock_prices"]
        
        session = self.spark.session
        
        # Ensure default namespace exists
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.default")
        
        # Check existing tables
        tables_df = session.sql(f"SHOW TABLES IN {self.catalog}.default")
        existing_tables = [row.tableName for row in tables_df.collect()]
        
        for table_name in tables_to_create:
            if table_name not in existing_tables:
                create_sql = self.generate_create_sql(table_name)
                if create_sql:
                    self.logger.info(f"Creating table {table_name} with SQL:\n{create_sql}")
                    session.sql(create_sql)
                    self.logger.info(f"Created table: {table_name}")
    
    def write_to_table(self, df: Any, table_name: str, mode: str = "append") -> bool:
        """Write data to an Iceberg table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode ("append", "overwrite", "merge")
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use Iceberg's capabilities for different write modes
            if mode == "append":
                df.writeTo(f"{self.catalog}.default.{table_name}").append()
                self.logger.info(f"Appended data to {table_name}")
            elif mode == "overwrite":
                df.writeTo(f"{self.catalog}.default.{table_name}").overwritePartitions()
                self.logger.info(f"Overwrote partitions in {table_name}")
            elif mode == "merge":
                # To be implemented
                self.logger.error(f"Merge mode not implemented for {table_name}")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Error writing to table {table_name}: {str(e)}")
            return False
    
    def load_table(self, table_name: str) -> Any:
        """Load an Iceberg table.
        
        Args:
            table_name: Name of the table to load
            
        Returns:
            Spark DataFrame with table data or empty DataFrame if table doesn't exist
        """
        try:
            return self.spark.session.table(f"{self.catalog}.default.{table_name}")
        except Exception as e:
            self.logger.error(f"Error loading table {table_name}: {str(e)}")
            return self.spark.session.createDataFrame([], self.schema.get_schema(table_name))
        
    def delete_table(self, table_name: str) -> bool:
        """Delete an Iceberg table.
        
        Args:
            table_name: Name of the table to delete
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.spark.session.sql(f"DROP TABLE IF EXISTS {self.catalog}.default.{table_name}")
            self.logger.info(f"Deleted table: {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting table {table_name}: {str(e)}")
            return False
    
    def get_snapshot_info(self, table_name: str) -> Dict[str, Any]:
        """Get snapshot information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with snapshot information
        """
        try:
            # Get snapshot information
            snapshot_df = self.spark.session.sql(
                f"SELECT * FROM {self.catalog}.default.{table_name}.snapshots"
            )
            snapshots = [{col: row[col] for col in snapshot_df.columns} 
                        for row in snapshot_df.collect()]
            
            # Get current snapshot ID
            current_snapshot_id = None
            if snapshots:
                current_snapshot_id = snapshots[-1].get("snapshot_id")
            
            return {
                "current_snapshot_id": current_snapshot_id,
                "snapshots": snapshots
            }
        except Exception as e:
            self.logger.error(f"Error getting snapshots for {table_name}: {str(e)}")
            return {"error": str(e)}