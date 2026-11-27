"""Transformação: Bronze -> Silver (tipagem, snake_case e MERGE incremental).

MERGE por id garante idempotência: reprocessar um lote não duplica dados.
O update só ocorre quando o hash do payload muda (detecta alteração real).
"""

import logging
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T

from src.config import BRONZE_PATH, SILVER_PATH

logger = logging.getLogger(__name__)

PRODUCTS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("title", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("discountPercentage", T.DoubleType()),
        T.StructField("rating", T.DoubleType()),
        T.StructField("stock", T.LongType()),
        T.StructField("brand", T.StringType()),
    ]
)

CARTS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("userId", T.LongType()),
        T.StructField("total", T.DoubleType()),
        T.StructField("discountedTotal", T.DoubleType()),
        T.StructField(
            "products",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("id", T.LongType()),
                        T.StructField("title", T.StringType()),
                        T.StructField("quantity", T.LongType()),
                        T.StructField("price", T.DoubleType()),
                        T.StructField("discountedPrice", T.DoubleType()),
                    ]
                )
            ),
        ),
    ]
)

USERS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("firstName", T.StringType()),
        T.StructField("lastName", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("phone", T.StringType()),
    ]
)

SCHEMAS = {"products": PRODUCTS_SCHEMA, "carts": CARTS_SCHEMA, "users": USERS_SCHEMA}


def camel_to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _silver_path(source: str) -> Path:
    return SILVER_PATH / source


def parse_bronze(spark: SparkSession, source: str):
    """Lê o Bronze, tipa o payload via from_json e aplica snake_case."""
    bronze = spark.read.format("delta").load(str(BRONZE_PATH / source))
    schema = SCHEMAS[source]

    parsed = bronze.withColumn("data", F.from_json("payload", schema))

    colunas_dados = [F.col(f"data.{campo}").alias(camel_to_snake(campo)) for campo in schema.fieldNames()]
    df = parsed.select(*colunas_dados, F.col("payload").alias("_payload"))

    return df.withColumn("_hash", F.md5(F.col("_payload"))).drop("_payload")


def transform_silver(spark: SparkSession, source: str) -> dict:
    """MERGE Bronze->Silver por id. Retorna métricas da carga (novos/atualizados)."""
    path = _silver_path(source)
    df = parse_bronze(spark, source).withColumn("updated_at", F.current_timestamp())
    df = df.dropDuplicates(["id"])

    registrou_antes = (path / "_delta_log").exists()
    antes = (
        spark.read.format("delta").load(str(path)).count() if registrou_antes else 0
    )

    if not registrou_antes:
        (path).mkdir(parents=True, exist_ok=True)
        df.write.format("delta").mode("overwrite").save(str(path))
        acao = "criada (primeira carga)"
    else:
        target = DeltaTable.forPath(spark, str(path))
        (
            target.alias("t")
            .merge(df.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll(condition="t._hash <> s._hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        acao = "mesclada (MERGE por id)"

    depois = spark.read.format("delta").load(str(path)).count()
    resultado = {
        "source": source,
        "acao": acao,
        "registros_antes": antes,
        "registros_depois": depois,
        "novos_ou_alterados": depois - antes,
    }
    logger.info("Silver %s: %s", source, resultado)
    return resultado
