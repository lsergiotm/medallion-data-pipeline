"""Orquestrador do pipeline: extract -> transform -> quality gate -> gold.

Uso:
    python -m src.run_pipeline --source all            # carga completa
    python -m src.run_pipeline --source products --incremental
    python -m src.run_pipeline --source carts
"""

import argparse
import logging
import sys

from src.config import SOURCES, get_spark, new_batch_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def run_source(spark, source: str, batch_id: str) -> None:
    """Executa extract e transform (MERGE) de uma fonte."""
    from src.extract import extract_to_bronze
    from src.transform import transform_silver

    n_bronze = extract_to_bronze(spark, source, batch_id)
    logger.info("[%s] bronze: %d registros no lote %s", source, n_bronze, batch_id[:8])

    metricas = transform_silver(spark, source)
    logger.info("[%s] silver: %s", source, metricas)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline ELT Medallion (Bronze/Silver/Gold)")
    parser.add_argument(
        "--source",
        choices=[*SOURCES.keys(), "all"],
        default="all",
        help="fonte a processar (default: all)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="não força nova extração: reprocessa o Bronze existente via MERGE",
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="executa apenas Bronze->Silver (sem rebuild da Gold)",
    )
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    fontes = list(SOURCES.keys()) if args.source == "all" else [args.source]
    batch_id = new_batch_id()
    logger.info("Batch %s | fontes=%s | incremental=%s", batch_id, fontes, args.incremental)

    try:
        for fonte in fontes:
            if args.incremental:
                from src.transform import transform_silver

                metricas = transform_silver(spark, fonte)
                logger.info("[%s] silver (incremental): %s", fonte, metricas)
            else:
                run_source(spark, fonte, batch_id)

        from src.quality import executar_quality_gate

        quality_silver = executar_quality_gate(spark, incluir_gold=False)
        logger.info("Quality (silver): %s", quality_silver["resumo"])

        if not args.skip_gold:
            from src.gold import build_gold

            contagens = build_gold(spark)
            logger.info("Gold: %s", contagens)

            quality_final = executar_quality_gate(spark, incluir_gold=True)
            logger.info("Quality (silver+gold): %s", quality_final["resumo"])

        logger.info("Pipeline concluído com sucesso.")
        return 0
    except Exception as exc:  # noqa: BLE001 — logging antes de encerrar
        logger.error("Pipeline FALHOU: %s", exc)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
