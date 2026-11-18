import os
import sys
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from amfi_lakehouse.config import HADOOP_HOME


def get_spark_session(app_name: str = "amfi-lakehouse") -> SparkSession:
    """Build a memory-safe local SparkSession configured with Delta Lake extensions."""
    if sys.platform.startswith("win") and HADOOP_HOME.exists():
        os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
        bin_path = str(HADOOP_HOME / "bin")
        if bin_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{bin_path};{os.environ.get('PATH', '')}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()