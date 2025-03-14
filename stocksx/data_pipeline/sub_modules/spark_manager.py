import os
import sys
import importlib.resources
import importlib.util
from pathlib import Path
from stocksx.configs.spark_config import SparkConfig
from pyspark.sql import SparkSession

class SparkManager:
    """Manages Apache Spark session."""
    def __init__(self, config: SparkConfig):
        """
        Initialize SparkManager with configurations.
        
        Args:
            config (SparkConfig): Configuration settings for Spark.
        """
        self.config = config
        self._session = None

    def _get_package_root(self):
        """Get the root directory of the stocksx package using importlib."""
        try:
            # Try to find the package location using importlib
            spec = importlib.util.find_spec('stocksx')
            if spec is not None and spec.submodule_search_locations:
                # Get the parent directory of the first location
                package_dir = Path(spec.submodule_search_locations[0])
                return package_dir.parent
        except (ImportError, AttributeError):
            pass
            
        # Fallback: try to find the package root from the current file
        try:
            current_file = Path(__file__)
            # Navigate up from submodules/data_pipeline to find package root
            for _ in range(4):  # Go up 4 levels
                if (current_file / 'stocksx').exists():
                    return current_file
                current_file = current_file.parent
        except Exception:
            pass
        
        # Final fallback: use the current working directory
        print("Warning: Could not determine package root, using current directory")
        return Path.cwd()

    def _get_absolute_path(self, path: str) -> str:
        """Get absolute path."""
        # Format iceberg_warehouse path
        if not path.startswith("file:///"):
            path = f"file:///{path.replace(os.sep, '/')}"
        return path
    
    @property
    def session(self) -> SparkSession:
        """Get or create a Spark session using Maven packages."""
        if self._session is None:
            # Get the package root directory
            package_root = self._get_package_root()
            iceberg_warehouse_path = package_root / self.config.iceberg_warehouse
            hive_warehouse_path = package_root / self.config.hive_warehouse
            
            # Convert to strings and get absolute paths
            self.config.iceberg_warehouse = self._get_absolute_path(str(iceberg_warehouse_path))
            self.config.hive_warehouse = self._get_absolute_path(str(hive_warehouse_path))
            print(f"Using package root: {package_root}")
            print(f"Iceberg warehouse: {self.config.iceberg_warehouse}")
            print(f"Hive warehouse: {self.config.hive_warehouse}")
            
            # Define configs directory path
            configs_dir = package_root / "stocksx" / "configs"
            os.makedirs(configs_dir, exist_ok=True)
            
            # Create maven packages string
            packages = []

            # Add Iceberg and PostgreSQL packages
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                packages.extend([
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
                    "org.apache.iceberg:iceberg-parquet:1.8.1"
                ])
            
            # Create hive-site.xml if metastore is enabled
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                # Create hive-site.xml directly in configs directory
                hive_site_path = configs_dir / "hive-site.xml"
                
                # Create hive-site.xml
                if not hive_site_path.exists():
                    with open(hive_site_path, "w") as f:
                        f.write(f"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
            <configuration>
            <property>
                <name>javax.jdo.option.ConnectionURL</name>
                <value>jdbc:postgresql://{self.config.hive_metastore_host}:{self.config.hive_metastore_port}/{self.config.hive_metastore_db}</value>
            </property>
            <property>
                <name>javax.jdo.option.ConnectionDriverName</name>
                <value>org.postgresql.Driver</value>
            </property>
            <property>
                <name>javax.jdo.option.ConnectionUserName</name>
                <value>{self.config.hive_metastore_user}</value>
            </property>
            <property>
                <name>javax.jdo.option.ConnectionPassword</name>
                <value>{self.config.hive_metastore_password}</value>
            </property>
            <property>
                <name>hive.metastore.schema.verification</name>
                <value>false</value>
            </property>
            <property>
                <name>hive.metastore.warehouse.dir</name>
                <value>{self.config.hive_warehouse}</value>
            </property>
            <property>
                <name>datanucleus.schema.autoCreateAll</name>
                <value>true</value>
            </property>
            <property>
                <name>datanucleus.schema.autoCreateTables</name>
                <value>true</value>
            </property>
            <property>
                <name>datanucleus.autoCreateSchema</name>
                <value>true</value>
            </property>
            <property>
                <name>datanucleus.autoCreateColumns</name>
                <value>true</value>
            </property>
            <property>
                <name>datanucleus.fixedDatastore</name>
                <value>false</value>
            </property>
            <property>
                <name>datanucleus.autoStartMechanismMode</name>
                <value>ignored</value>
            </property>
            <property>
                <name>datanucleus.readOnlyDatastore</name>
                <value>false</value>
            </property>
            <property>
                <name>datanucleus.rdbms.initializeColumnInfo</name>
                <value>NONE</value>
            </property>
            <property>
                <name>hive.metastore.schema.verification.record.version</name>
                <value>false</value>
            </property>
            </configuration>""")
                
                # Set environment variable to point to the configs directory
                os.environ["HIVE_CONF_DIR"] = str(configs_dir)
            
            # Start building the session
            builder = SparkSession.builder.appName(self.config.app_name)
            
            # Add packages if specified
            if packages:
                builder = builder.config("spark.jars.packages", ",".join(packages))
            
            # Add all standard Spark configs
            builder = builder \
                .config("spark.sql.execution.arrow.pyspark.enabled", self.config.arrow_enabled) \
                .config("spark.sql.shuffle.partitions", self.config.shuffle_partitions) \
                .config("spark.default.parallelism", self.config.parallelism) \
                .config("spark.executor.memory", self.config.executor_memory) \
                .config("spark.driver.memory", self.config.driver_memory) \
                .config("spark.network.timeout", self.config.network_timeout) \
                .config("spark.executor.heartbeatInterval", self.config.heartbeat_interval) \
                .config("spark.worker.timeout", self.config.worker_timeout) \
                .config("spark.akka.timeout", self.config.lookup_timeout) \
                .config("spark.akka.askTimeout", self.config.ask_timeout) \
                .config("spark.serializer", self.config.serializer) \
                .config("spark.kryo.registrationRequired", self.config.kryo_registration_required) \
                .config("spark.master", self.config.master)
            
            # Add Hive/Iceberg specific configs
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                builder = builder \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                    .config("spark.sql.catalog.spark_catalog.type", "hive") \
                    .config("spark.sql.warehouse.dir", self.config.iceberg_warehouse)
            
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                # Find the PostgreSQL JDBC driver JAR in the package structure
                libs_dir = package_root / "stocksx" / "libs"
                jdbc_jar_name = "postgresql-42.7.5.jar"
                jdbc_jar_path = libs_dir / jdbc_jar_name
                
                # Check if the driver exists
                if jdbc_jar_path.exists():
                    print(f"Found PostgreSQL JDBC driver at: {jdbc_jar_path}")
                    # Use absolute path with proper OS-specific separators
                    builder = builder.config("spark.driver.extraClassPath", str(jdbc_jar_path))
                    builder = builder.config("spark.executor.extraClassPath", str(jdbc_jar_path))
                else:
                    print(f"PostgreSQL JDBC driver not found at expected location: {jdbc_jar_path}")
                    print("Creating directory structure if needed...")
                    
                    # Create the libs directory if it doesn't exist
                    os.makedirs(libs_dir, exist_ok=True)
                    
                    # Try to copy from current directory or download if needed
                    current_dir_jar = Path.cwd() / jdbc_jar_name
                    if current_dir_jar.exists():
                        print(f"Found JAR in current directory, copying to: {jdbc_jar_path}")
                        import shutil
                        shutil.copy2(current_dir_jar, jdbc_jar_path)
                    else:
                        print(f"Downloading PostgreSQL JDBC driver to: {jdbc_jar_path}")
                        try:
                            import urllib.request
                            url = f"https://jdbc.postgresql.org/download/{jdbc_jar_name}"
                            urllib.request.urlretrieve(url, jdbc_jar_path)
                            print(f"Downloaded PostgreSQL JDBC driver to: {jdbc_jar_path}")
                        except Exception as e:
                            print(f"Warning: Failed to download PostgreSQL JDBC driver: {e}")
                            # Fall back to Maven package if download fails
                            packages.append("org.postgresql:postgresql:42.7.5")
                            print("Will rely on Maven packages for the driver")
                    # If the file now exists (after copy/download), use it
                    if jdbc_jar_path.exists():
                        builder = builder.config("spark.driver.extraClassPath", str(jdbc_jar_path))
                        builder = builder.config("spark.executor.extraClassPath", str(jdbc_jar_path))
                
                # Enable Hive support
                builder = builder.enableHiveSupport()
                
                # Point to Hive config directory
                builder = builder.config("spark.hadoop.hive.metastore.warehouse.dir", self.config.hive_warehouse)
                
                # Add explicit JDBC driver configuration
                builder = builder.config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
                builder = builder.config("spark.driver.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
            
            # Add garbage collector settings
            for key, value in self.config.garbage_collectors.items():
                builder = builder.config(key, value)
            
            # Create the session
            self._session = builder.getOrCreate()
            
            # Set log level for better debugging
            self._session.sparkContext.setLogLevel("INFO")
            
            # Print confirmation of active configs
            print(f"Created Spark session with:")
            print(f"- Iceberg enabled: {self.config.iceberg_enabled if hasattr(self.config, 'iceberg_enabled') else False}")
            print(f"- Hive metastore: {self.config.hive_metastore_enabled if hasattr(self.config, 'hive_metastore_enabled') else False}")
            print(f"- Warehouse dir: {self.config.iceberg_warehouse}")
            print(f"- Hive config dir: {os.environ.get('HIVE_CONF_DIR', 'Not set')}")
            
        return self._session
    
    def initialize_hive_metastore_schema(self):
        """Initialize the Hive metastore schema in the PostgreSQL database."""
        try:
            print("Initializing Hive metastore schema...")
            
            # Import required libraries
            import psycopg2
            
            # Connect to PostgreSQL server
            conn = psycopg2.connect(
                host=self.config.hive_metastore_host,
                port=self.config.hive_metastore_port,
                user=self.config.hive_metastore_user,
                password=self.config.hive_metastore_password,
                database=self.config.hive_metastore_db
            )
            
            cur = conn.cursor()
            
            # Check if core tables exist
            cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'next_lock_id'
            );
            """)
            has_tables = cur.fetchone()[0]
            
            if not has_tables:
                print("Creating core tables manually...")
                
                # Get schema file path
                package_root = self._get_package_root()
                warehouse_dir = package_root / self.config.hive_warehouse
                os.makedirs(warehouse_dir, exist_ok=True)
                schema_file = warehouse_dir / "hive-schema-3.1.0.postgres.sql"
                
                # Download schema file if it doesn't exist
                if not schema_file.exists():
                    try:
                        import urllib.request
                        print(f"Downloading Hive schema SQL to {schema_file}...")
                        url = "https://raw.githubusercontent.com/apache/hive/rel/release-3.1.0/standalone-metastore/src/main/sql/postgres/hive-schema-3.1.0.postgres.sql"
                        urllib.request.urlretrieve(url, schema_file)
                    except Exception as e:
                        print(f"Failed to download schema file: {e}")
                        return False
                
                # Execute the schema initialization SQL
                if schema_file.exists():
                    with open(schema_file, 'r') as f:
                        sql = f.read()
                        # PostgreSQL requires each statement to be executed separately
                        # Split on semicolons but preserve them in statements
                        statements = []
                        current_statement = ""
                        for line in sql.splitlines():
                            line = line.strip()
                            if not line or line.startswith("--"):
                                continue
                            
                            current_statement += line + " "
                            if line.endswith(";"):
                                statements.append(current_statement)
                                current_statement = ""
                        
                        # Execute each statement
                        for statement in statements:
                            if statement.strip():
                                try:
                                    cur.execute(statement)
                                    conn.commit()
                                except psycopg2.Error as e:
                                    print(f"Warning: Error executing statement: {e}")
                                    # Continue with the next statement
                    
                    print("Hive metastore schema initialized successfully!")
                else:
                    print("Schema file not found. Schema initialization failed.")
                    return False
            else:
                print("Hive metastore schema already exists.")
            
            cur.close()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Failed to initialize Hive metastore schema: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
    def verify_configuration(self):
        """Verify that the configuration is working correctly."""
        try:
            print("\n=== Verifying Configuration ===")
            
            # Test basic Spark functionality
            print("Testing Spark basic functionality...")
            result = self.session.sql("SELECT 1 + 1").collect()[0][0]
            print(f"Basic Spark functionality: OK (1+1={result})")
            
            # Initialize Hive metastore if enabled
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                print("\nInitializing Hive metastore schema...")
                self.initialize_hive_metastore_schema()
            
            # Test Iceberg extensions if enabled
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                print("\nTesting Iceberg functionality...")
                try:
                    self.session.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.iceberg_test")
                    self.session.sql("""
                    CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_test.test_table (
                        id INT,
                        name STRING
                    ) USING iceberg
                    """)
                    print("Iceberg functionality: OK")
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"Iceberg functionality: FAILED - {str(e)}")
                    
                    # Try again after initializing schema
                    print("Attempting to fix by initializing schema...")
                    if self.initialize_hive_metastore_schema():
                        try:
                            self.session.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.iceberg_test")
                            self.session.sql("""
                            CREATE TABLE IF NOT EXISTS spark_catalog.iceberg_test.test_table (
                                id INT,
                                name STRING
                            ) USING iceberg
                            """)
                            print("Iceberg functionality: OK (after schema initialization)")
                        except Exception as e2:
                            print(f"Iceberg still not working after schema initialization: {e2}")
            
            # Test Hive metastore if enabled
            # Check the actual column names in the result
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                print("\nTesting Hive metastore...")
                try:
                    databases = self.session.sql("SHOW DATABASES").collect()
                    # Print schema to debug column names
                    print("Database result schema:")
                    self.session.sql("SHOW DATABASES").printSchema()
                    
                    # Get the first column name dynamically
                    first_column = self.session.sql("SHOW DATABASES").schema.names[0]
                    db_names = [getattr(db, first_column) for db in databases]
                    print(f"Found databases: {db_names}")
                    print("Hive metastore functionality: OK")

                    # Drop test database if it exists
                    if "iceberg_test" in db_names:
                        self.session.sql("DROP DATABASE iceberg_test CASCADE")
                        print("Dropped test database")
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"Hive metastore functionality: FAILED - {str(e)}")
            print("\nVerification complete!")
            return True
        except Exception as e:
            print(f"Verification failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
    def cleanup(self):
        """Cleanup resources and stop the Spark session."""
        if self._session is not None:
            self._session.stop()
            self._session = None
            print("Spark session stopped and cleaned up.")
        else:
            print("No active Spark session to clean up.")