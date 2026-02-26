"""
spark_utils.py
--------------
Módulo utilitário compartilhado por todos os scripts de pipeline.
Centraliza a criação da SparkSession para evitar duplicação de configuração.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str, memory: str = "4g") -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para o Data Lake.

    Args:
        app_name: Nome da aplicação Spark (aparece no log e Spark UI).
        memory: Memória do executor. Default: '4g'.

    Returns:
        SparkSession pronta para uso.

    Exemplo:
        from spark_utils import get_spark_session
        spark = get_spark_session("IngestaoBronzeENEM")
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.executor.memory", memory)
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", "8")  # Reduz o padrão 200 para datasets menores
        .getOrCreate()
    )
    # Reduz verbosidade dos logs do Spark
    spark.sparkContext.setLogLevel("WARN")
    return spark
