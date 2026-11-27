# Medallion Data Pipeline — ELT de API REST para Lakehouse Delta Lake

[Read in English](README.en.md)

Pipeline ELT que consome uma API REST de e-commerce e popula um lakehouse com arquitetura **Medallion (Bronze/Silver/Gold)** usando **PySpark + Delta Lake**, com cargas incrementais idempotentes (MERGE), validações de qualidade de dados e camada Gold pronta para BI.

---

## Contexto do Problema

Uma empresa de varejo recebe os dados de pedidos e catálogo de um parceiro marketplace através de uma **API REST paginada**. O time de BI precisa desses dados consolidados, confiáveis e históricos para vendas, catálogo e comportamento de compra — mas consumir API direto no dashboard é inviável: lentidão, ausência de histórico, sem deduplicação e sem garantia de qualidade.

O papel do pipeline: **transformar uma integração frágil em um lakehouse confiável**, com Camada Bronze (imutável, "como veio da API"), Silver (limpa, deduplicada, confiável) e Gold (agregações prontas para consumo), como se faz em ambientes reais de Fabric e Databricks.

## Objetivo

1. Extrair dados da API com paginação correta e metadados de auditoria (batch, timestamp).
2. Garantir **idempotência**: reprocessar um lote não duplica dados (MERGE por chave).
3. Padronizar e tipar os dados (camelCase da API -> snake_case analítico).
4. Validar qualidade por regras explícitas (chaves, nulos, valores, integridade).
5. Publicar agregações Gold (vendas por categoria, KPIs gerais) prontas para Power BI/SQL.

## Fonte dos Dados

**DummyJSON** (`https://dummyjson.com`) — API REST pública de demonstração, estável e sem autenticação.

- Endpoints usados: `/products` (catálogo), `/carts` (pedidos/carrinhos), `/users` (clientes).
- Paginação padrão `limit`/`skip` com campo `total` na resposta.
- Cenário de negócio simulado: dados do "parceiro marketplace" a serem consolidados no lakehouse da varejista.
- Nenhum dado confidencial — API pública para demonstração, substituível por qualquer API interna sem mudança de arquitetura (basta apontar `API_BASE_URL` em `src/config.py`).

## Arquitetura / Metodologia

```mermaid
flowchart TB
    subgraph EXTRACT["Extract (Python + requests)"]
        A[API REST DummyJSON\nproducts | carts | users\npaginação limit/skip]
    end
    subgraph LAKEHOUSE["Lakehouse Delta Lake (data/lakehouse)"]
        B[(BRONZE\npayload JSON cru + metadados\nde auditoria - append imutável)]
        C[(SILVER\ndados tipados, snake_case,\ndeduplicados via MERGE)]
        D[(GOLD\ndim_produto, fato_pedido_item,\nagregações de vendas/KPIs)]
    end
    E{{Quality Gate\nregras de qualidade\n-> quality_report.json}}
    F[BI: Power BI / SQL Analytics]

    A --> B --> C --> D --> F
    C --> E
    D --> E
    E --> |falha crítica: aborta| X((Pipeline para))
    E --> |ok| F
```

**Responsabilidade de cada camada** (arquitetura Medallion):

| Camada | Formato | Papel | Regra de escrita |
|---|---|---|---|
| **Bronze** | Delta | Histórico imutável "como veio da API" (payload JSON + `record_id`, `batch_id`, `ingested_at`) | Append — nunca reescrever |
| **Silver** | Delta | Dados confiáveis para análise: tipados, `snake_case`, deduplicados | **MERGE por chave** — idempotente |
| **Gold** | Delta | Modelagem para consumo: dimensões, fatos e KPIs | Overwrite (recalculada a cada execução) |

**Estratégia de carga incremental:** a cada execução, apenas os registros novos/alterados fazem MERGE no Silver (comparação por hash do payload — atualiza `updated_at` somente quando algo muda); o Bronze acumula o histórico de lotes para rastreabilidade. Rodar o pipeline 2x seguidas não altera o resultado Silver/Gold: carga **idempotente**, a mesma semântica de MERGE usada em pipelines de Fabric/Databricks.

## Tecnologias Utilizadas

- **Python 3.11 + PySpark 3.5** — processamento distribuído
- **Delta Lake (delta-spark 3.2)** — ACID, MERGE, time travel no lakehouse
- **requests** — consumo da API com paginação
- **Spark SQL** — transformações e agregações da camada Gold

Diagramas detalhados, contratos de dados e decisões de arquitetura: [`docs/arquitetura.md`](docs/arquitetura.md).

## Tratamento Realizado (Bronze -> Silver)

| Aspecto | Decisão |
|---|---|
| Payload cru | Bronze preserva o JSON original (**time travel** e reprocessamento sem nova chamada à API) |
| Tipagem | `from_json` com schema explícito por endpoint — erro de tipo vira null audável, nunca falha silenciosa |
| Nomes | camelCase da API -> snake_case analítico |
| Deduplicação | MERGE por chave (`id`) no Silver: insert de novos, update só quando o hash do registro muda |
| Auditoria | `record_id` (hash determinístico), `batch_id`, `ingested_at` (Bronze) e `updated_at` (Silver) |
| Chaves | `id` por entidade; `fato_pedido_item` liga pedidos a produtos (explode do array de itens) |

## Análises Desenvolvidas (Camada Gold)

| Tabela | Conteúdo |
|---|---|
| `gold.dim_produto` | Catálogo padronizado: id, título, marca, categoria, preço, rating, estoque |
| `gold.fato_pedido_item` | 1 linha por item de pedido: pedido, cliente, produto, quantidade, receita |
| `gold.agg_vendas_categoria` | Receita, itens, pedidos e rating médio por categoria |
| `gold.agg_kpis_gerais` | Receita total, ticket médio, nº de pedidos, produtos e clientes |

## KPIs / Métricas

- **Receita total** (soma `quantidade x preço` dos itens pedidos)
- **Ticket médio** (receita / nº de pedidos)
- **Pedidos, produtos e clientes** cobertos pela integração
- **Receita e rating por categoria** (priorização de catálogo)
- **Métricas do pipeline:** registros por batch, novos vs. atualizados no MERGE, regras de qualidade OK/falha por execução

## Resultados

Execução local real (Python 3.11, PySpark 3.5.1, Delta 3.2, Java 11) — reproduza com `python -m src.run_pipeline --source all`:

| Camada | Resultado |
|---|---|
| Bronze | 610 registros ingeridos no lote (products 194, carts 208, users 208), payload original + `batch_id`/`ingested_at` |
| Silver | 194 + 208 + 208 registros tipados e deduplicados via MERGE |
| Gold | `dim_produto` 194 · `fato_pedido_item` 800 · `agg_vendas_categoria` 24 · `agg_kpis_gerais` 1 |
| Quality gate | 10/10 regras OK (0 críticas) — `data/lakehouse/quality/quality_report.json` |

**Idempotência comprovada:** uma segunda execução (`--incremental`) reprocessou os mesmos 610 registros e gravou **0 mudanças** no Silver (`novos_ou_alterados: 0` nas três fontes), com a Gold reconstruída idêntica.

**KPIs na Gold após a carga (dados de demonstração da API):**

| KPI | Valor |
|---|---|
| Receita total (itens pedidos) | 3.834.278,63 |
| Ticket médio | 18.434,03 |
| Pedidos / clientes | 208 / 208 |
| Produtos distintos pedidos | 189 (de 194 no catálogo) |

Top categorias por receita: `vehicle` (1,88M), `motorcycle` (670k) e `mens-watches` (645k) — ranking completo na tabela `agg_vendas_categoria`.

## Insights de Negócio (exemplos de leitura da Gold)

1. **Categorias líderes** em receita e rating (ex.: fragrances/skincare em alta no catálogo de demonstração) orientam destaque na home do e-commerce.
2. **Ticket médio e profundidade de desconto** por pedido revelam se as promoções estão concentrando receita ou apenas corroendo margem.
3. **Produtos pedidos fora do catálogo atual** (quebrariam a integridade fato x dim) são barrados pelo quality gate — sinal de dessincronização entre API e pipeline.
4. **Reprocessamento zero-dano:** lotes podem ser reprocessados para recuperação de falha sem duplicar receita — requisito de confiabilidade para reportar números ao board.

## Diagramas / Screenshots

- Arquitetura completa: Mermaid acima + [`docs/arquitetura.md`](docs/arquitetura.md).
- Histórico de lotes e qualidade: saídas em `data/lakehouse/` após execução.

> **Screenshot (placeholder):** substituir por print real das tabelas Delta no lakehouse (ex.: Databricks Catalog / Fabric Lakehouse Explorer).

## Como Executar

Pré-requisitos: **Python 3.11** e **Java 11/17** (necessário para o Spark).

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Carga completa (todos os endpoints) ou individual
python -m src.run_pipeline --source all
python -m src.run_pipeline --source products --incremental

# Habilitar incremental (padrão: full na primeira execução)
python -m src.run_pipeline --source carts --incremental
```

Saídas: lakehouse em `data/lakehouse/{bronze,silver,gold}`, relatório de qualidade em `data/lakehouse/quality/quality_report.json`.

> **Nota para Windows:** o Spark local também precisa do `winutils.exe` (baixe a versão 3.3.x em [cdarlint/winutils](https://github.com/cdarlint/winutils) e aponte a variável `HADOOP_HOME` para a pasta que contém `bin\winutils.exe`). Em redes corporativas que bloqueiam o repositório Maven, coloque os JARs do Delta (`delta-spark_2.12` e `delta-storage` da sua versão) em `jars/` — o `src/config.py` os injeta via `spark.jars` automaticamente. Use **Python 3.11**: o PySpark 3.5.x ainda não suporta 3.12+.

## Próximos Passos

- Orquestração agendada (Airflow, Fabric Data Pipeline ou Databricks Workflows).
- Testes unitários com `pytest` + fixtures (`tests/fixtures/` já inclui amostra de payload).
- SCD Tipo 2 no dim_produto (histórico de preços por data).
- Monitoramento/alerta (e-mail/Teams) sobre o quality report.
- Deploy no Microsoft Fabric com OneLake/Shortcut para consumo direto do Power BI.

---

**Autor:** [Luís Sérgio Teixeira Mesquita](https://www.linkedin.com/in/luis-sergiotm/) · [github.com/lsergiotm](https://github.com/lsergiotm)
