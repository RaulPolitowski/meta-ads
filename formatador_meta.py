"""
Módulo de formatação dos dados para Meta Ads Custom Audience.
Gera CSV compatível e aplica hashing SHA-256 nos campos PII para a API.
"""

import csv
import hashlib
import os
import re

import config


def _normalize(value: str) -> str:
    """Normaliza string: lowercase, strip, remove espaços extras."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


def _hash_sha256(value: str) -> str:
    """Aplica SHA-256 a um valor normalizado. Retorna string vazia se vazio."""
    normalized = _normalize(value)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _format_phone(phone: str) -> str:
    """Remove caracteres não numéricos e adiciona código do país."""
    digits = re.sub(r"\D", "", phone)
    if not digits:
        return ""
    if not digits.startswith("55"):
        digits = "55" + digits
    return digits


def _format_cnpj(cnpj: str) -> str:
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r"\D", "", cnpj)


def formatar_para_csv(empresas: list[dict]) -> str:
    """
    Gera CSV compatível com Meta Ads Custom Audience (dados em texto plano).

    Colunas: email, phone, fn (first name), ln (last name), ct (city),
             st (state), zip, country, company.

    Returns:
        Caminho do arquivo CSV gerado.
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(config.OUTPUT_DIR, config.CSV_FILENAME)

    headers = ["email", "phone", "fn", "ln", "ct", "st", "zip", "country", "company"]

    rows = []
    for emp in empresas:
        socio_parts = emp.get("socio_nome", "").strip().split(" ", 1)
        first_name = socio_parts[0] if socio_parts else ""
        last_name = socio_parts[1] if len(socio_parts) > 1 else ""

        phone = _format_phone(emp.get("telefone1", ""))
        if not phone:
            phone = _format_phone(emp.get("telefone2", ""))

        row = {
            "email": emp.get("email", "").strip().lower(),
            "phone": phone,
            "fn": first_name.strip().lower(),
            "ln": last_name.strip().lower(),
            "ct": emp.get("municipio", "").strip().lower(),
            "st": emp.get("uf", "").strip().lower(),
            "zip": re.sub(r"\D", "", emp.get("cep", "")),
            "country": "br",
            "company": emp.get("razao_social", "").strip().lower(),
        }
        rows.append(row)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[Formatador] CSV gerado: {filepath} ({len(rows)} registros)")
    return filepath


def formatar_para_api(empresas: list[dict]) -> list[dict]:
    """
    Formata dados com hashing SHA-256 para uso direto na Meta Marketing API.

    Cada registro contém campos PII com hash SHA-256 conforme exigido pela API.

    Returns:
        Lista de dicts com campos hasheados.
    """
    registros_hash: list[dict] = []

    for emp in empresas:
        socio_parts = emp.get("socio_nome", "").strip().split(" ", 1)
        first_name = socio_parts[0] if socio_parts else ""
        last_name = socio_parts[1] if len(socio_parts) > 1 else ""

        phone = _format_phone(emp.get("telefone1", ""))
        if not phone:
            phone = _format_phone(emp.get("telefone2", ""))

        registro = {
            "email": _hash_sha256(emp.get("email", "")),
            "phone": _hash_sha256(phone),
            "fn": _hash_sha256(first_name),
            "ln": _hash_sha256(last_name),
            "ct": _hash_sha256(emp.get("municipio", "")),
            "st": _hash_sha256(emp.get("uf", "")),
            "zip": _hash_sha256(re.sub(r"\D", "", emp.get("cep", ""))),
            "country": _hash_sha256("br"),
        }

        # Só inclui registro se tiver pelo menos email ou telefone
        if registro["email"] or registro["phone"]:
            registros_hash.append(registro)

    print(f"[Formatador] {len(registros_hash)} registros formatados com hash para API.")
    return registros_hash


def gerar_estatisticas(empresas: list[dict]) -> dict:
    """Gera estatísticas sobre os dados extraídos."""
    total = len(empresas)
    com_email = sum(1 for e in empresas if e.get("email", "").strip())
    com_telefone = sum(
        1 for e in empresas
        if e.get("telefone1", "").strip() or e.get("telefone2", "").strip()
    )
    com_socio = sum(1 for e in empresas if e.get("socio_nome", "").strip())

    # Contagem por UF
    por_uf: dict[str, int] = {}
    for e in empresas:
        uf = e.get("uf", "N/A")
        por_uf[uf] = por_uf.get(uf, 0) + 1

    # Contagem por porte
    por_porte: dict[str, int] = {}
    for e in empresas:
        porte = e.get("porte", "N/A")
        por_porte[porte] = por_porte.get(porte, 0) + 1

    return {
        "total_empresas": total,
        "com_email": com_email,
        "com_telefone": com_telefone,
        "com_socio": com_socio,
        "percentual_com_email": round(com_email / total * 100, 2) if total else 0,
        "percentual_com_telefone": round(com_telefone / total * 100, 2) if total else 0,
        "por_uf": dict(sorted(por_uf.items(), key=lambda x: x[1], reverse=True)),
        "por_porte": por_porte,
    }
