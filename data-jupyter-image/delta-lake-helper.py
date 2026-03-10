"""
Delta Lake helper functions for Jupyter notebooks.
Automatically loaded in notebooks via startup script.
"""

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_delta_spark_session(
    app_name="DeltaLakeSpark",
    executor_instances=2,
    executor_memory="2g",
    executor_cores=1,
    minio_endpoint=None,
    minio_access_key=None,
    minio_secret_key=None
):
    """
    Create a Spark session with Delta Lake support on Kubernetes.
    
    Parameters:
    -----------
    app_name : str
        Name of the Spark application
    executor_instances : int
        Number of executor pods
    executor_memory : str
        Memory per executor (e.g., "2g", "4g")
    executor_cores : int
        CPU cores per executor
    minio_endpoint : str, optional
        MinIO endpoint (default: from env MINIO_ENDPOINT)
    minio_access_key : str, optional
        MinIO access key (default: from env MINIO_ACCESS_KEY)
    minio_secret_key : str, optional
        MinIO secret key (default: from env MINIO_SECRET_KEY)
    
    Returns:
    --------
    SparkSession with Delta Lake enabled
    """
    
    # Get MinIO credentials
    minio_endpoint = minio_endpoint or os.getenv("MINIO_ENDPOINT")
    minio_access_key = minio_access_key or os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = minio_secret_key or os.getenv("MINIO_SECRET_KEY")
    
    # Get pod hostname for driver
    pod_hostname = os.getenv("HOSTNAME")
    driver_host = f"{pod_hostname}.jupyterhub.svc.cluster.local"
    
    print("🚀 Creating Delta Lake Spark Session on Kubernetes")
    print(f"   Application: {app_name}")
    print(f"   Driver: {driver_host}")
    print(f"   Executors: {executor_instances} × {executor_memory} ({executor_cores} cores)")
    print(f"   MinIO: {minio_endpoint}")
    print(f"   Delta Lake: ✅ Enabled")
    
    # Create Spark builder with Delta Lake extensions
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("k8s://https://kubernetes.default.svc:443") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.kubernetes.container.image", "apache/spark:4.0.0") \
        .config("spark.kubernetes.namespace", "spark-jobs") \
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "jupyter-spark") \
        .config("spark.executor.instances", str(executor_instances)) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.driver.host", driver_host) \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    
    # Configure Delta Lake via configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    print(f"✅ Spark {spark.version} with Delta Lake started!")
    print(f"📊 Spark UI: http://localhost:4040")
    print()
    
    return spark


def show_delta_table_info(spark, table_path):
    """
    Show Delta table information including version history.
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    table_path : str
        Path to Delta table (e.g., "s3a://bucket/path/to/table")
    """
    from delta.tables import DeltaTable
    
    print(f"📊 Delta Table Information: {table_path}")
    print("=" * 80)
    
    # Load Delta table
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Show current data
    print("\n📁 Current Data:")
    delta_table.toDF().show(10)
    
    # Show history
    print("\n📜 Version History:")
    delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(10, truncate=False)
    
    # Show details
    print("\n🔍 Table Details:")
    delta_table.detail().show(truncate=False)


def vacuum_delta_table(spark, table_path, retention_hours=168):
    """
    Vacuum a Delta table to remove old files.
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    table_path : str
        Path to Delta table
    retention_hours : int
        Retention period in hours (default: 168 = 7 days)
    """
    from delta.tables import DeltaTable
    
    print(f"🧹 Vacuuming Delta table: {table_path}")
    print(f"   Retention: {retention_hours} hours")
    
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.vacuum(retention_hours)
    
    print("✅ Vacuum complete!")


# Print helper message when imported
print("✅ Delta Lake helpers loaded!")
print("   Usage:")
print("     spark = create_delta_spark_session()")
print("     show_delta_table_info(spark, 's3a://bucket/table')")
print("     vacuum_delta_table(spark, 's3a://bucket/table')")