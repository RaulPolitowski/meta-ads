"""
Módulo de upload automático via Meta Marketing API.
Cria Custom Audiences e faz upload de dados em batches.
"""

import json
import time

import requests

import config


def _meta_url(path: str) -> str:
    """Monta URL completa da Meta API."""
    return f"{config.META_BASE_URL}/{path}"


def _meta_headers() -> dict:
    """Headers padrão (token vai como parâmetro, não header)."""
    return {"Content-Type": "application/json"}


def _check_token():
    """Verifica se o token de acesso está configurado."""
    if not config.META_ACCESS_TOKEN:
        raise ValueError(
            "META_ACCESS_TOKEN não configurado. "
            "Defina a variável de ambiente META_ACCESS_TOKEN ou configure em config.py."
        )


def criar_audience(name: str, description: str = "") -> dict:
    """
    Cria uma Custom Audience no Meta Ads.

    Args:
        name: Nome da audience.
        description: Descrição opcional.

    Returns:
        Resposta da API com o ID da audience criada.
    """
    _check_token()

    url = _meta_url(f"act_{config.META_AD_ACCOUNT_ID}/customaudiences")

    payload = {
        "name": name,
        "subtype": "CUSTOM",
        "description": description or f"Audience criada automaticamente - {name}",
        "customer_file_source": "USER_PROVIDED_ONLY",
        "access_token": config.META_ACCESS_TOKEN,
    }

    response = requests.post(url, json=payload, timeout=30)
    result = response.json()

    if "error" in result:
        raise RuntimeError(
            f"Erro ao criar audience: {result['error'].get('message', result['error'])}"
        )

    audience_id = result.get("id", "")
    print(f"[Uploader] Audience criada: ID={audience_id}, Nome={name}")
    return result


def _build_batch_payload(registros: list[dict]) -> dict:
    """Monta o payload de um batch para a API."""
    schema_fields = ["EMAIL", "PHONE", "FN", "LN", "CT", "ST", "ZIP", "COUNTRY"]
    field_map = {
        "EMAIL": "email",
        "PHONE": "phone",
        "FN": "fn",
        "LN": "ln",
        "CT": "ct",
        "ST": "st",
        "ZIP": "zip",
        "COUNTRY": "country",
    }

    data_rows = []
    for reg in registros:
        row = [reg.get(field_map[field], "") for field in schema_fields]
        data_rows.append(row)

    return {
        "schema": schema_fields,
        "data": data_rows,
    }


def upload_audience(audience_id: str, registros_hash: list[dict], verbose: bool = True) -> dict:
    """
    Faz upload de dados hasheados para uma Custom Audience em batches.

    Args:
        audience_id: ID da audience no Meta.
        registros_hash: Lista de registros com dados PII hasheados.
        verbose: Se True, imprime progresso.

    Returns:
        Dict com estatísticas do upload.
    """
    _check_token()

    url = _meta_url(f"{audience_id}/users")
    total = len(registros_hash)
    batches_enviados = 0
    registros_enviados = 0
    erros = []

    for i in range(0, total, config.META_BATCH_SIZE):
        batch = registros_hash[i : i + config.META_BATCH_SIZE]
        batch_num = (i // config.META_BATCH_SIZE) + 1

        payload_data = _build_batch_payload(batch)

        payload = {
            "payload": json.dumps(payload_data),
            "access_token": config.META_ACCESS_TOKEN,
        }

        if verbose:
            print(f"[Uploader] Enviando batch {batch_num} ({len(batch)} registros)...")

        try:
            response = requests.post(url, json=payload, timeout=60)
            result = response.json()

            if "error" in result:
                error_msg = result["error"].get("message", str(result["error"]))
                erros.append({"batch": batch_num, "error": error_msg})
                print(f"[Uploader] Erro no batch {batch_num}: {error_msg}")
            else:
                registros_enviados += len(batch)
                batches_enviados += 1
                num_received = result.get("num_received", len(batch))
                if verbose:
                    print(f"[Uploader] Batch {batch_num} enviado. Recebidos: {num_received}")
        except requests.RequestException as e:
            erros.append({"batch": batch_num, "error": str(e)})
            print(f"[Uploader] Erro de conexão no batch {batch_num}: {e}")

        # Delay entre batches para evitar rate limiting
        if i + config.META_BATCH_SIZE < total:
            time.sleep(1)

    stats = {
        "total_registros": total,
        "registros_enviados": registros_enviados,
        "batches_enviados": batches_enviados,
        "erros": erros,
    }

    if verbose:
        print(f"[Uploader] Upload finalizado. "
              f"Enviados: {registros_enviados}/{total}. Erros: {len(erros)}")

    return stats


def verificar_audience(audience_id: str) -> dict:
    """
    Verifica o status de uma Custom Audience.

    Args:
        audience_id: ID da audience no Meta.

    Returns:
        Dados da audience.
    """
    _check_token()

    url = _meta_url(audience_id)
    params = {
        "fields": "id,name,approximate_count,operation_status,data_source,delivery_status",
        "access_token": config.META_ACCESS_TOKEN,
    }

    response = requests.get(url, params=params, timeout=30)
    result = response.json()

    if "error" in result:
        raise RuntimeError(
            f"Erro ao verificar audience: {result['error'].get('message', result['error'])}"
        )

    print(f"[Uploader] Audience {audience_id}:")
    print(f"  Nome: {result.get('name', 'N/A')}")
    print(f"  Tamanho aprox: {result.get('approximate_count', 'N/A')}")
    print(f"  Status: {result.get('operation_status', {})}")

    return result


def listar_audiences() -> list[dict]:
    """
    Lista todas as Custom Audiences da conta.

    Returns:
        Lista de audiences.
    """
    _check_token()

    url = _meta_url(f"act_{config.META_AD_ACCOUNT_ID}/customaudiences")
    params = {
        "fields": "id,name,approximate_count,time_created,time_updated,operation_status",
        "limit": 100,
        "access_token": config.META_ACCESS_TOKEN,
    }

    response = requests.get(url, params=params, timeout=30)
    result = response.json()

    if "error" in result:
        raise RuntimeError(
            f"Erro ao listar audiences: {result['error'].get('message', result['error'])}"
        )

    audiences = result.get("data", [])

    print(f"[Uploader] {len(audiences)} audiences encontradas:")
    for aud in audiences:
        print(f"  - {aud.get('id')}: {aud.get('name')} "
              f"(~{aud.get('approximate_count', 'N/A')} registros)")

    return audiences
