import os
from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def test_delta_spark_local():
    # Point Hadoop home to our local Windows binaries
    hadoop_dir = Path(__file__).resolve().parents[1] / "hadoop"
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["PATH"] = f"{hadoop_dir / 'bin'};{os.environ['PATH']}"

    builder = (
        SparkSession.builder.appName("amfi-hello-world")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        df = spark.createDataFrame([(1, "HDFC Top 100")], ["scheme_code", "scheme_name"])
        assert df.count() == 1
        print("Spark + Delta initialized successfully!")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_delta_spark_local()