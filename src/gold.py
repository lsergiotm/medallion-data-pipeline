"""Camada Gold: modelagem de consumo (dimensão, fato e agregações) via Spark SQL.

Sobrescrita a cada execução (agregações derivadas — mais barato recalcular).
A lógica equivalente em SQL puro está documentada em sql/gold_queries.sql.
"""

import logging

from pyspark.sql import SparkSession

from src.config import GOLD_PATH, SILVER_PATH

logger = logging.getLogger(__name__)


def build_gold(spark: SparkSession) -> dict:
    """Reconstrói a camada Gold a partir da Silver. Retorna contagem por tabela."""
    silver = SILVER_PATH.as_posix()
    gold = GOLD_PATH.as_posix()

    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_products AS SELECT * FROM delta.`{silver}/products`")
    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_carts AS SELECT * FROM delta.`{silver}/carts`")
    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_users AS SELECT * FROM delta.`{silver}/users`")

    # ------------------------------------------------------------------
    # OSS Delta: CREATE OR REPLACE TABLE via caminho não é suportado pelo
    # catálogo padrão; o equivalente idempotente é sobrescrever via API
    # (mode("overwrite") no mesmo caminho reconstrói a tabela).
    # ------------------------------------------------------------------
    def escreve_gold(tabela: str, consulta_sql: str) -> None:
        spark.sql(consulta_sql).write.format("delta").mode("overwrite").save(
            f"{gold}/{tabela}"
        )

    # ------------------------------------------------------------------
    # Dimensão de produto (catálogo padronizado)
    # Nota: os subcampos do array em silver_carts preservam os nomes da API
    # (camelCase), pois from_json mapeia por NOME. O renomeio acontece aqui.
    # ------------------------------------------------------------------
    escreve_gold(
        "dim_produto",
        f"""
        SELECT
            id,
            title                        AS titulo,
            COALESCE(brand, 'sem marca') AS marca,
            category                     AS categoria,
            price                        AS preco,
            discount_percentage          AS desconto_pct,
            rating,
            stock                        AS estoque
        FROM silver_products
        """,
    )

    # ------------------------------------------------------------------
    # Fato: 1 linha por item de pedido (explode do array de produtos)
    # ------------------------------------------------------------------
    escreve_gold(
        "fato_pedido_item",
        f"""
        SELECT
            c.id                       AS pedido_id,
            c.user_id                  AS cliente_id,
            item.id                    AS produto_id,
            item.title                 AS titulo_produto,
            item.quantity              AS quantidade,
            item.price                 AS preco_unitario,
            item.discountedPrice       AS preco_com_desconto,
            item.quantity * item.price AS receita_item,
            c.total,
            c.discounted_total
        FROM silver_carts c
        LATERAL VIEW explode(c.products) itens AS item
        """,
    )

    # ------------------------------------------------------------------
    # Agregação: vendas por categoria
    # ------------------------------------------------------------------
    escreve_gold(
        "agg_vendas_categoria",
        f"""
        SELECT
            d.categoria,
            SUM(f.receita_item)         AS receita,
            SUM(f.quantidade)           AS qtd_itens,
            COUNT(DISTINCT f.pedido_id) AS qtd_pedidos,
            ROUND(AVG(d.rating), 2)     AS rating_medio
        FROM delta.`{gold}/fato_pedido_item` f
        JOIN delta.`{gold}/dim_produto` d ON f.produto_id = d.id
        GROUP BY d.categoria
        """,
    )

    # ------------------------------------------------------------------
    # KPIs gerais (linha única — cartão de painel)
    # ------------------------------------------------------------------
    escreve_gold(
        "agg_kpis_gerais",
        f"""
        SELECT
            SUM(f.receita_item)                                          AS receita_total,
            COUNT(DISTINCT f.pedido_id)                                  AS qtd_pedidos,
            ROUND(SUM(f.receita_item) / COUNT(DISTINCT f.pedido_id), 2)  AS ticket_medio,
            COUNT(DISTINCT f.produto_id)                                 AS qtd_produtos,
            (SELECT COUNT(DISTINCT id) FROM silver_users)                AS qtd_clientes
        FROM delta.`{gold}/fato_pedido_item` f
        """,
    )

    contagens = {}
    for tabela in ["dim_produto", "fato_pedido_item", "agg_vendas_categoria", "agg_kpis_gerais"]:
        contagens[tabela] = spark.sql(
            f"SELECT COUNT(*) AS n FROM delta.`{gold}/{tabela}`"
        ).first()["n"]
    logger.info("Gold reconstruída: %s", contagens)
    return contagens
