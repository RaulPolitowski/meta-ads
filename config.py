"""
Configurações para automação Meta Ads.
Variáveis sensíveis devem ser definidas via variáveis de ambiente ou arquivo .env.
"""

import os

# ── Meta Ads ──────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "341072783558983")
META_API_VERSION = "v19.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

# ── Casa dos Dados (extração de CNPJs) ───────────────────────────────────────
CASADOSDADOS_API_URL = (
    "https://api.casadosdados.com.br/v2/public/cnpj/search"
)

# Filtros de busca
ESTADOS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

CNAES = []  # Lista de CNAEs para filtrar (vazio = todos)

PORTES = [
    "MICRO EMPRESA",
    "EMPRESA DE PEQUENO PORTE",
    "DEMAIS",
]

NATUREZAS_JURIDICAS = []  # Vazio = todas

MESES_RETROATIVOS = 3  # Buscar empresas abertas nos últimos N meses

# ── Paginação / Rate Limiting ────────────────────────────────────────────────
CASADOSDADOS_PAGE_SIZE = 20
CASADOSDADOS_MAX_PAGES = 50
CASADOSDADOS_DELAY_SECONDS = 2  # Delay entre requisições para evitar rate limit

# ── Meta Ads Upload ──────────────────────────────────────────────────────────
META_BATCH_SIZE = 10000  # Registros por batch no upload

# ── Caminhos de saída ────────────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
CSV_FILENAME = "custom_audience.csv"
REPORT_FILENAME = "relatorio.json"

# ── Agendamento ──────────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_HOURS = 24  # Intervalo para modo --agendar
