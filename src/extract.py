"""Extração: API REST -> Bronze (Delta, append-only, com metadados de auditoria).

O payload é serializado de forma canônica (sort_keys=True) para que o hash
md5 seja estável entre execuções — base da comparação no MERGE do Silver.
"""

import json
import logging

import requests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T

from src.config import (
    API_BASE_URL,
    BRONZE_PATH,
    PAGE_SIZE,
    SOURCES,
    new_batch_id,
)

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("record_id", T.StringType(), False),
        T.StructField("source_endpoint", T.StringType(), False),
        T.StructField("record_pk", T.StringType(), False),
        T.StructField("payload", T.StringType(), False),
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
    ]
)


def canonical_payload(registro: dict) -> str:
    """JSON string determinístico (mesma saída para o mesmo conteúdo)."""
    return json.dumps(registro, sort_keys=True, ensure_ascii=False)


def fetch_all(source: str) -> list:
    """Busca todos os registros do endpoint, respeitando a paginação limit/skip."""
    cfg = SOURCES[source]
    registros, skip = [], 0
    while True:
        resp = requests.get(
            f"{API_BASE_URL}{cfg['endpoint']}",
            params={"limit": PAGE_SIZE, "skip": skip},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        chunk = body.get(cfg["root_key"], [])
        registros.extend(chunk)
        total = body.get("total", len(registros))
        skip += PAGE_SIZE
        if not chunk or skip >= total:
            break
    logger.info("[%s] %d registros baixados da API", source, len(registros))
    return registros


def extract_to_bronze(spark: SparkSession, source: str, batch_id: str = None) -> int:
    """Baixa a fonte completa e grava no Bronze (append imutável).

    Retorna a quantidade de registros persistidos no lote.
    """
    batch_id = batch_id or new_batch_id()
    cfg = SOURCES[source]
    registros = fetch_all(source)

    linhas = [
        (source, str(reg.get("id")), canonical_payload(reg))
        for reg in registros
    ]

    df = spark.createDataFrame(linhas, ["source_endpoint", "record_pk", "payload"])
    df = (
        df.withColumn(
            "record_id", F.md5(F.concat_ws(":", F.col("source_endpoint"), F.col("record_pk")))
        )
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        .select("record_id", "source_endpoint", "record_pk", "payload", "batch_id", "ingested_at")
    )

    path = str(BRONZE_PATH / source)
    df.write.format("delta").mode("append").save(path)
    return len(linhas)
