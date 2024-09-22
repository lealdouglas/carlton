# spark_session_manager.py
from pyspark.sql import SparkSession


class SparkSessionManager:
    @staticmethod
    def get_spark_session() -> SparkSession:
        """
        Obtém ou cria uma sessão Spark.
        Get or create a Spark session.

        Returns:
            SparkSession: A sessão Spark.
                          The Spark session.
        """
        return SparkSession.builder.getOrCreate()
