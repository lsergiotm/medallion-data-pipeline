"""Configuração central: sessão Spark, caminhos do lakehouse e endpoints da API."""

import uuid
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
LAKEHOUSE = ROOT / "data" / "lakehouse"

BRONZE_PATH = LAKEHOUSE / "bronze"
SILVER_PATH = LAKEHOUSE / "silver"
GOLD_PATH = LAKEHOUSE / "gold"
QUALITY_PATH = LAKEHOUSE / "quality"

API_BASE_URL = "https://dummyjson.com"
PAGE_SIZE = 50

SOURCES = {
    "products": {"endpoint": "/products", "root_key": "products"},
    "carts": {"endpoint": "/carts", "root_key": "carts"},
    "users": {"endpoint": "/users", "root_key": "users"},
}


def new_batch_id() -> str:
    """Identificador único do lote de execução (auditoria no Bronze)."""
    return str(uuid.uuid4())


def get_spark(app_name: str = "medallion-data-pipeline") -> SparkSession:
    """Sessão Spark configurada com as extensões do Delta Lake.

    Padrão: configure_spark_with_delta_pip resolve os JARs do delta-spark
    instalado via pip (baixando do Maven na primeira execução).

    Fallback offline: se os JARs do Delta existirem em jars/, eles são
    adicionados via spark.jars — útil em redes corporativas que bloqueiam
    repositórios Maven.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )
    jars_dir = ROOT / "jars"
    delta_jars = sorted(jars_dir.glob("delta-*.jar")) if jars_dir.exists() else []
    if delta_jars:
        builder = builder.config("spark.jars", ",".join(str(p) for p in delta_jars))
        return builder.getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()
