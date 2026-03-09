#!/usr/bin/env python3
"""
Orquestrador da automação Meta Ads.

Modos de uso:
    python main.py                    # Fluxo completo (extrair + formatar + upload)
    python main.py --apenas-extrair   # Apenas extrai e salva dados
    python main.py --apenas-upload    # Apenas faz upload de dados já extraídos
    python main.py --listar-audiences # Lista audiences existentes na conta
    python main.py --agendar          # Executa em loop com agendamento
"""

import argparse
import json
import os
import sys
from datetime import datetime

import config
import extrator_cnpj
import formatador_meta
import uploader_meta


def _salvar_relatorio(stats: dict, filepath: str | None = None) -> str:
    """Salva relatório JSON com estatísticas da execução."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    if filepath is None:
        filepath = os.path.join(config.OUTPUT_DIR, config.REPORT_FILENAME)

    stats["timestamp"] = datetime.now().isoformat()

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[Main] Relatório salvo em {filepath}")
    return filepath


def fluxo_extrair(verbose: bool = True) -> list[dict]:
    """Executa apenas a etapa de extração."""
    print("=" * 60)
    print("[Main] Iniciando extração de empresas...")
    print("=" * 60)

    empresas = extrator_cnpj.extrair_empresas(verbose=verbose)

    if not empresas:
        print("[Main] Nenhuma empresa encontrada.")
        return []

    extrator_cnpj.salvar_empresas_json(empresas)
    csv_path = formatador_meta.formatar_para_csv(empresas)

    stats = formatador_meta.gerar_estatisticas(empresas)
    stats["csv_path"] = csv_path
    _salvar_relatorio({"extracao": stats})

    print(f"[Main] Extração concluída. {len(empresas)} empresas encontradas.")
    return empresas


def fluxo_upload(empresas: list[dict] | None = None, audience_name: str | None = None, verbose: bool = True) -> dict:
    """Executa apenas a etapa de upload para o Meta."""
    print("=" * 60)
    print("[Main] Iniciando upload para Meta Ads...")
    print("=" * 60)

    # Se não recebeu empresas, tenta carregar do JSON salvo
    if empresas is None:
        raw_path = os.path.join(config.OUTPUT_DIR, "empresas_raw.json")
        if not os.path.exists(raw_path):
            print(f"[Main] Arquivo não encontrado: {raw_path}")
            print("[Main] Execute primeiro com --apenas-extrair")
            return {}

        with open(raw_path, "r", encoding="utf-8") as f:
            empresas = json.load(f)

    if not empresas:
        print("[Main] Nenhuma empresa para upload.")
        return {}

    # Formatar dados com hash
    registros_hash = formatador_meta.formatar_para_api(empresas)

    if not registros_hash:
        print("[Main] Nenhum registro válido para upload (sem email/telefone).")
        return {}

    # Criar audience
    if audience_name is None:
        audience_name = f"Empresas Novas - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    audience_result = uploader_meta.criar_audience(
        name=audience_name,
        description=f"Empresas abertas nos últimos {config.MESES_RETROATIVOS} meses",
    )
    audience_id = audience_result.get("id")

    if not audience_id:
        print("[Main] Falha ao criar audience.")
        return {}

    # Upload em batches
    upload_stats = uploader_meta.upload_audience(
        audience_id=audience_id,
        registros_hash=registros_hash,
        verbose=verbose,
    )

    # Verificar status
    audience_status = uploader_meta.verificar_audience(audience_id)

    # Relatório
    result = {
        "audience_id": audience_id,
        "audience_name": audience_name,
        "upload_stats": upload_stats,
        "audience_status": audience_status,
    }

    _salvar_relatorio({"upload": result})

    print(f"[Main] Upload concluído. Audience ID: {audience_id}")
    return result


def fluxo_completo(audience_name: str | None = None, verbose: bool = True) -> dict:
    """Executa o fluxo completo: extração + formatação + upload."""
    print("=" * 60)
    print("[Main] Executando fluxo completo...")
    print("=" * 60)

    empresas = fluxo_extrair(verbose=verbose)
    if not empresas:
        return {}

    upload_result = fluxo_upload(
        empresas=empresas,
        audience_name=audience_name,
        verbose=verbose,
    )

    # Relatório consolidado
    stats_extracao = formatador_meta.gerar_estatisticas(empresas)
    relatorio = {
        "extracao": stats_extracao,
        "upload": upload_result,
    }
    _salvar_relatorio(relatorio)

    return relatorio


def fluxo_agendar(audience_name: str | None = None, verbose: bool = True):
    """Executa o fluxo completo em loop com agendamento."""
    try:
        import schedule
    except ImportError:
        print("[Main] Módulo 'schedule' não instalado. Execute: pip install schedule")
        sys.exit(1)

    import time

    print("=" * 60)
    print(f"[Main] Modo agendado. Executando a cada {config.SCHEDULE_INTERVAL_HOURS}h")
    print("=" * 60)

    def job():
        print(f"\n[Main] Execução agendada - {datetime.now().isoformat()}")
        fluxo_completo(audience_name=audience_name, verbose=verbose)

    # Executar imediatamente na primeira vez
    job()

    schedule.every(config.SCHEDULE_INTERVAL_HOURS).hours.do(job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n[Main] Agendamento interrompido pelo usuário.")


def main():
    parser = argparse.ArgumentParser(
        description="Automação Meta Ads - Extração de CNPJs e upload de Custom Audiences"
    )
    parser.add_argument(
        "--apenas-extrair",
        action="store_true",
        help="Apenas extrai dados da API Casa dos Dados e gera CSV",
    )
    parser.add_argument(
        "--apenas-upload",
        action="store_true",
        help="Apenas faz upload de dados já extraídos (requer extração prévia)",
    )
    parser.add_argument(
        "--listar-audiences",
        action="store_true",
        help="Lista todas as Custom Audiences da conta Meta",
    )
    parser.add_argument(
        "--agendar",
        action="store_true",
        help=f"Executa em loop a cada {config.SCHEDULE_INTERVAL_HOURS}h",
    )
    parser.add_argument(
        "--audience-name",
        type=str,
        default=None,
        help="Nome customizado para a audience",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduz output no console",
    )

    args = parser.parse_args()
    verbose = not args.quiet

    if args.listar_audiences:
        uploader_meta.listar_audiences()
    elif args.apenas_extrair:
        fluxo_extrair(verbose=verbose)
    elif args.apenas_upload:
        fluxo_upload(audience_name=args.audience_name, verbose=verbose)
    elif args.agendar:
        fluxo_agendar(audience_name=args.audience_name, verbose=verbose)
    else:
        fluxo_completo(audience_name=args.audience_name, verbose=verbose)


if __name__ == "__main__":
    main()
