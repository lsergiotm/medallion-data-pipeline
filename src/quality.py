"""Quality gate: validações de qualidade sobre Silver e Gold.

Cada regra registra resultado (ok/falha, severidade, detalhe) no relatório
data/lakehouse/quality/quality_report.json. Falhas CRÍTICAS abortam o pipeline
antes que dados não validados alcancem os consumidores.
"""

import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from src.config import GOLD_PATH, QUALITY_PATH, SILVER_PATH

logger = logging.getLogger(__name__)

TOLERANCIA_TOTAL = 0.05


class QualidadeCriticaError(RuntimeError):
    """Levantada quando uma regra crítica falha."""


def _check(spark, regras, nome, tabela, severidade, query_falhas=None, df_falhas=None, detalhe_ok="0 violações"):
    """Executa uma regra e anexa o resultado à lista."""
    if df_falhas is None and query_falhas is not None:
        df_falhas = spark.sql(query_falhas)
    violacoes = df_falhas.count() if df_falhas is not None else 0
    regras.append(
        {
            "regra": nome,
            "tabela": tabela,
            "severidade": severidade,
            "resultado": "ok" if violacoes == 0 else "falha",
            "violacoes": violacoes,
            "detalhe": detalhe_ok if violacoes == 0 else f"{violacoes} violação(ões)",
        }
    )


def validar_silver(spark: SparkSession, regras: list) -> None:
    """Regras críticas sobre a Silver — devem passar antes do rebuild da Gold."""
    silver = SILVER_PATH.as_posix()

    for fonte in ["products", "carts", "users"]:
        df = spark.sql(f"SELECT * FROM delta.`{silver}/{fonte}`")

        _check(
            spark,
            regras,
            nome=f"[{fonte}] chave primária nula",
            tabela=f"silver.{fonte}",
            severidade="critica",
            df_falhas=df.filter(F.col("id").isNull()),
        )
        _check(
            spark,
            regras,
            nome=f"[{fonte}] duplicidade de chave",
            tabela=f"silver.{fonte}",
            severidade="critica",
            df_falhas=df.groupBy("id").count().filter(F.col("count") > 1).select("id", "count"),
        )

    produtos = spark.sql(f"SELECT * FROM delta.`{silver}/products`")
    _check(
        spark,
        regras,
        nome="[products] preço negativo",
        tabela="silver.products",
        severidade="critica",
        df_falhas=produtos.filter(F.col("price") < 0),
    )

    carts = spark.sql(f"SELECT * FROM delta.`{silver}/carts`")
    itens = (
        carts.select("id", "total", F.explode("products").alias("item"))
        .groupBy("id", "total")
        .agg(F.sum(F.col("item.quantity") * F.col("item.price")).alias("total_calculado"))
    )
    _check(
        spark,
        regras,
        nome="[carts] total divergente da soma dos itens",
        tabela="silver.carts",
        severidade="warning",
        df_falhas=itens.filter(
            F.abs(F.col("total") - F.col("total_calculado")) > TOLERANCIA_TOTAL
        ),
        detalhe_ok=f"divergências <= {TOLERANCIA_TOTAL}",
    )


def validar_gold(spark: SparkSession, regras: list) -> None:
    """Regras de integridade sobre a Gold recém-reconstruída."""
    gold = GOLD_PATH.as_posix()

    orfaos = spark.sql(
        f"""
        SELECT f.produto_id
        FROM delta.`{gold}/fato_pedido_item` f
        LEFT ANTI JOIN delta.`{gold}/dim_produto` d ON f.produto_id = d.id
        """
    )
    _check(
        spark,
        regras,
        nome="[gold] itens de pedido sem produto no catálogo (fato x dim)",
        tabela="gold.fato_pedido_item",
        severidade="critica",
        df_falhas=orfaos,
    )

    kpis = spark.sql(f"SELECT * FROM delta.`{gold}/agg_kpis_gerais`").collect()
    receita = kpis[0]["receita_total"] if kpis else None
    pedidos = kpis[0]["qtd_pedidos"] if kpis else 0
    _check(
        spark,
        regras,
        nome="[gold] KPIs consistentes (receita > 0 quando há pedidos)",
        tabela="gold.agg_kpis_gerais",
        severidade="critica",
        df_falhas=None,
        detalhe_ok=f"receita_total={receita}",
    )
    regras[-1]["resultado"] = "ok" if (receita or 0) > 0 or pedidos == 0 else "falha"
    if regras[-1]["resultado"] == "falha":
        regras[-1]["violacoes"] = 1
        regras[-1]["detalhe"] = f"receita_total={receita} com {pedidos} pedidos"


def executar_quality_gate(spark: SparkSession, incluir_gold: bool) -> dict:
    """Executa as validações, grava o relatório e aborta em falha crítica."""
    regras: list = []

    validar_silver(spark, regras)
    if incluir_gold:
        validar_gold(spark, regras)

    report = {
        "executado_em": datetime.now(timezone.utc).isoformat(),
        "incluir_gold": incluir_gold,
        "resumo": {
            "total": len(regras),
            "ok": sum(1 for r in regras if r["resultado"] == "ok"),
            "falhas": sum(1 for r in regras if r["resultado"] == "falha"),
            "criticas_falhas": sum(
                1 for r in regras if r["resultado"] == "falha" and r["severidade"] == "critica"
            ),
        },
        "regras": regras,
    }

    QUALITY_PATH.mkdir(parents=True, exist_ok=True)
    destino = QUALITY_PATH / "quality_report.json"
    destino.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info("Quality report: %s -> %s", report["resumo"], destino)

    if report["resumo"]["criticas_falhas"] > 0:
        criticas = [r for r in regras if r["resultado"] == "falha" and r["severidade"] == "critica"]
        raise QualidadeCriticaError(
            f"{len(criticas)} regra(s) crítica(s) falharam: "
            + "; ".join(f"{r['regra']} ({r['detalhe']})" for r in criticas)
        )
    return report
