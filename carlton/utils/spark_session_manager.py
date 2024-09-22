# spark_session_manager.py
from pyspark.sql import SparkSession


class SparkSessionManager:
    @staticmethod
    def create_spark_session(app_name):
        """
        Cria ou obtém uma sessão Spark.
        Creates or gets a Spark session.

        Args:
            app_name (str): Nome do aplicativo Spark.
                            Spark application name.

        Returns:
            SparkSession: A sessão Spark.
                        The Spark session.
        """
        return SparkSession.builder.appName(app_name).getOrCreate()
