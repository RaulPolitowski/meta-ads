"""
Módulo de extração de CNPJs de empresas novas via API da Casa dos Dados.
"""

import json
import os
import time
from datetime import datetime, timedelta

import requests

import config


def _build_date_range(meses_retroativos: int) -> tuple[str, str]:
    """Retorna (data_inicio, data_fim) no formato YYYY-MM-DD."""
    hoje = datetime.now()
    inicio = hoje - timedelta(days=meses_retroativos * 30)
    return inicio.strftime("%Y-%m-%d"), hoje.strftime("%Y-%m-%d")


def _build_query(page: int = 1) -> dict:
    """Monta o payload de busca para a API da Casa dos Dados."""
    data_inicio, data_fim = _build_date_range(config.MESES_RETROATIVOS)

    query: dict = {
        "query": {
            "termo": [],
            "atividade_principal": config.CNAES if config.CNAES else [],
            "natureza_juridica": config.NATUREZAS_JURIDICAS if config.NATUREZAS_JURIDICAS else [],
            "uf": config.ESTADOS if config.ESTADOS else [],
            "municipio": [],
            "bairro": [],
            "situacao_cadastral": "ATIVA",
            "cep": [],
            "ddd": [],
        },
        "range_query": {
            "data_inicio": data_inicio,
            "data_fim": data_fim,
        },
        "extras": {
            "somente_mei": False,
            "excluir_mei": False,
            "com_email": False,
            "incluir_atividade_secundaria": False,
            "com_contato_telefonico": False,
            "somente_celular": False,
            "somente_fixo": False,
            "somente_matriz": False,
            "somente_filial": False,
        },
        "page": page,
    }

    if config.PORTES:
        query["query"]["porte"] = config.PORTES

    return query


def _parse_empresa(raw: dict) -> dict:
    """Extrai campos relevantes de um registro bruto."""
    socios = raw.get("socios", [])
    socio_nome = socios[0].get("nome", "") if socios else ""

    return {
        "cnpj": raw.get("cnpj", ""),
        "razao_social": raw.get("razao_social", ""),
        "nome_fantasia": raw.get("nome_fantasia", ""),
        "email": raw.get("email", ""),
        "telefone1": raw.get("ddd_telefone_1", ""),
        "telefone2": raw.get("ddd_telefone_2", ""),
        "socio_nome": socio_nome,
        "logradouro": raw.get("logradouro", ""),
        "numero": raw.get("numero", ""),
        "complemento": raw.get("complemento", ""),
        "bairro": raw.get("bairro", ""),
        "municipio": raw.get("municipio", ""),
        "uf": raw.get("uf", ""),
        "cep": raw.get("cep", ""),
        "cnae_fiscal": raw.get("cnae_fiscal", ""),
        "cnae_fiscal_descricao": raw.get("cnae_fiscal_descricao", ""),
        "porte": raw.get("porte", ""),
        "data_inicio_atividade": raw.get("data_inicio_atividade", ""),
    }


def extrair_empresas(verbose: bool = True) -> list[dict]:
    """
    Consulta a API da Casa dos Dados e retorna lista de empresas.

    Returns:
        Lista de dicts com dados das empresas extraídas.
    """
    todas_empresas: list[dict] = []
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "MetaAdsAutomation/1.0",
    }

    for page in range(1, config.CASADOSDADOS_MAX_PAGES + 1):
        payload = _build_query(page)

        if verbose:
            print(f"[Extrator] Buscando página {page}...")

        try:
            response = requests.post(
                config.CASADOSDADOS_API_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[Extrator] Erro na requisição (página {page}): {e}")
            break

        data = response.json()
        registros = data.get("data", {}).get("cnpj", [])

        if not registros:
            if verbose:
                print("[Extrator] Sem mais registros. Finalizando.")
            break

        for registro in registros:
            empresa = _parse_empresa(registro)
            todas_empresas.append(empresa)

        if verbose:
            print(f"[Extrator] {len(registros)} registros na página {page}. "
                  f"Total acumulado: {len(todas_empresas)}")

        time.sleep(config.CASADOSDADOS_DELAY_SECONDS)

    if verbose:
        print(f"[Extrator] Extração finalizada. Total: {len(todas_empresas)} empresas.")

    return todas_empresas


def salvar_empresas_json(empresas: list[dict], filepath: str | None = None) -> str:
    """Salva a lista de empresas em JSON. Retorna o caminho do arquivo."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    if filepath is None:
        filepath = os.path.join(config.OUTPUT_DIR, "empresas_raw.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(empresas, f, ensure_ascii=False, indent=2)

    print(f"[Extrator] Dados salvos em {filepath}")
    return filepath
