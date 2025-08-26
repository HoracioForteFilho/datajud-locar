"""DataJud Locar pipeline (v2.1)

This script collects information about legal cases related to a specified
company/individual from the Brazilian National Justice Council (CNJ) public
API.  It handles paging, rate‑limiting via retries with backoff, extracts
deadline ("prazos"), decision summaries, and execution phase indicators
from movement descriptions, and exports the consolidated results to a
variety of formats.

Example usage:

    export DATAJUD_API_KEY="<your secret key>"
    python datajud_locar_pipeline_v2.py \
        --nome "LOCAR SANEAMENTO AMBIENTAL LTDA" \
        --cnpj 35474949000108 \
        --tribunais tjpe tjba tjsp trf5 \
        --max-paginas 25 \
        --excel processos_locar.xlsx \
        --pdf processos_locar.pdf \
        --sqlite processos_locar.db \
        --csv processos_locar.csv

The script will automatically skip PDF generation if the ``fpdf2`` package
is not installed or when ``--no-pdf`` is supplied. A ``--selftest`` flag
is also provided to validate the export logic without making network
requests.

"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sqlite3
import sys
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

# Attempt to import FPDF (pdf export); fallback gracefully
try:
    from fpdf import FPDF  # type: ignore
    _HAS_FPDF = True
except Exception:
    FPDF = None  # type: ignore
    _HAS_FPDF = False

try:
    from requests.adapters import HTTPAdapter, Retry
except Exception:
    HTTPAdapter = None  # type: ignore
    Retry = None  # type: ignore


# Default API configuration
DEFAULT_API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
BASE_URL = "https://api-publica.datajud.cnj.jus.br/api_publica_"

# Keywords used to classify process movements
KEYWORDS_PRAZO = [
    "prazo",
    "intimação",
    "manifestação",
    "apresentar defesa",
    "apresentar",
    "contestação",
    "contrarrações",
    "embargos",
    "notificação",
    "resposta",
    "recurso",
    "juntada de petição",
]

KEYWORDS_DECISAO = [
    "sentença",
    "acórdão",
    "decisão",
    "despacho",
    "homologação",
    "julgado",
    "julgamento",
    "proferida",
    "deferida",
    "indeferida",
]

KEYWORDS_EXECUCAO = [
    "execução",
    "cumprimento",
    "penhora",
    "bloqueio",
    "exequente",
    "exequido",
    "expedição de alvará",
    "leilão",
    "arrematação",
]


def create_session(max_retries: int = 5) -> requests.Session:
    """Create a requests session with retry/backoff configured.

    The CNJ API occasionally returns 429 or 5xx responses.  Configure
    retries with exponential backoff to mitigate these transient errors.

    Parameters
    ----------
    max_retries : int
        Maximum number of retry attempts for failed requests.

    Returns
    -------
    requests.Session
        A session object with retry logic mounted on HTTPS requests.
    """
    session = requests.Session()
    # Set a custom User‑Agent to avoid generic requests header
    session.headers.update({
        "User-Agent": "DataJudLocarPipeline/2.1 (+https://github.com)",
    })
    if HTTPAdapter is not None and Retry is not None:
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    return session


def parse_date(date_str: str) -> Optional[_dt.date]:
    """Parse an ISO‑like date string into a date object if possible."""
    try:
        return _dt.date.fromisoformat(date_str[:10])
    except Exception:
        return None


def buscar_processos(
    session: requests.Session,
    nome: str,
    cnpj: str,
    tribunal: str,
    max_paginas: int,
    desde: Optional[_dt.date] = None,
) -> List[Dict[str, any]]:
    """Collect process data for a given tribunal.

    Parameters
    ----------
    session : requests.Session
        A configured HTTP session.
    nome : str
        Party name to search for.
    cnpj : str
        Party document (CNPJ/CPF) to search for.
    tribunal : str
        Tribunal code (lowercase) such as ``tjpe``.
    max_paginas : int
        Maximum number of result pages to query.
    desde : Optional[datetime.date]
        Skip processes distributed before this date.

    Returns
    -------
    List[Dict[str, any]]
        A list of dictionaries with extracted process information.
    """
    endpoint = f"{BASE_URL}{tribunal}/_search"
    resultados: List[Dict[str, any]] = []
    page_size = 100
    for pagina in range(max_paginas):
        body = {
            "size": page_size,
            "from": pagina * page_size,
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"partes.nome": nome}},
                        {"match": {"partes.documento": cnpj}},
                    ],
                },
            },
        }
        try:
            resp = session.post(endpoint, json=body, timeout=50)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"[WARN] Falha ao consultar {tribunal.upper()} página {pagina+1}: {exc}")
            break
        hits: List[dict] = data.get("hits", {}).get("hits", [])
        if not hits:
            # No more data
            break
        for p in hits:
            src: dict = p.get("_source", {})
            # Skip processes distributed before 'desde' if specified
            dt_dist = parse_date(src.get("dataDistribuicao", ""))
            if desde and dt_dist and dt_dist < desde:
                continue
            movimentos = src.get("movimentos", [])
            # Extract deadlines (prazos) from movement descriptions
            prazos: List[str] = []
            for m in movimentos:
                desc = m.get("descricao", "").lower()
                if any(keyword in desc for keyword in KEYWORDS_PRAZO):
                    timestamp = m.get("dataHora", "")
                    prazos.append(f"{timestamp}: {m.get('descricao', '')}")
            prazos_txt = "\n".join(prazos)
            # Extract last decision summary
            resumo_decisao: str = ""
            for m in reversed(movimentos):
                desc = m.get("descricao", "").lower()
                if any(keyword in desc for keyword in KEYWORDS_DECISAO):
                    resumo_decisao = m.get("descricao", "")
                    break
            # Check if in execution phase
            fase_execucao = any(
                keyword in m.get("descricao", "").lower()
                for m in movimentos
                for keyword in KEYWORDS_EXECUCAO
            )
            resultados.append({
                "cnj": src.get("numeroProcesso"),
                "tribunal": tribunal.upper(),
                "grau": src.get("grau"),
                "classe": src.get("classeProcessual"),
                "assunto": ", ".join(src.get("assuntosProcessuais", []) or []),
                "orgao": src.get("orgaoJulgador", {}).get("nomeOrgao"),
                "status": src.get("situacaoProcessual"),
                "partes": "; ".join(
                    f"{parte.get('tipoParte', '')}: {parte.get('nome', '')}"
                    for parte in src.get("partes", [])
                ),
                "dt_distribuicao": src.get("dataDistribuicao"),
                "qtd_movimentos": len(movimentos),
                "prazos_relevantes": prazos_txt,
                "resumo_decisao": resumo_decisao,
                "fase_execucao": "Sim" if fase_execucao else "Não",
            })
        # If fewer than page_size results, assume no more pages
        if len(hits) < page_size:
            break
    return resultados


def exportar_excel(dados: Iterable[Dict[str, any]], caminho: str) -> None:
    """Export data to an Excel file using openpyxl via pandas."""
    df = pd.DataFrame(dados)
    df.to_excel(caminho, index=False)


def exportar_csv(dados: Iterable[Dict[str, any]], caminho: str) -> None:
    """Export data to a CSV file."""
    df = pd.DataFrame(dados)
    df.to_csv(caminho, index=False)


def exportar_sqlite(dados: Iterable[Dict[str, any]], caminho: str) -> None:
    """Export data to a SQLite database."""
    df = pd.DataFrame(dados)
    with sqlite3.connect(caminho) as conn:
        df.to_sql("processos", conn, if_exists="replace", index=False)


def exportar_pdf(dados: Iterable[Dict[str, any]], caminho: str) -> None:
    """Export data to a simple PDF report.

    If the FPDF library is not available, raise RuntimeError so that
    callers can handle the absence gracefully.
    """
    if not _HAS_FPDF:
        raise RuntimeError("PDF export requested but fpdf2 is not installed.")
    pdf = FPDF(format="A4")  # type: ignore[name-defined]
    pdf.set_auto_page_break(auto=True, margin=15)
    for item in dados:
        pdf.add_page()
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 8, "Relatório de Processo", 0, 1, "C")
        pdf.ln(4)
        pdf.set_font("Arial", "", 10)
        for chave, valor in item.items():
            texto = f"{chave}: {valor if valor else '-'}"
            pdf.multi_cell(0, 6, texto)
        pdf.ln(2)
    pdf.output(caminho)


def exportar_txt(dados: Iterable[Dict[str, any]], caminho: str) -> None:
    """Export data to a plain text report as a fallback when PDF is unavailable."""
    with open(caminho, "w", encoding="utf-8") as f:
        for idx, item in enumerate(dados, start=1):
            f.write(f"Processo #{idx}\n")
            for chave, valor in item.items():
                f.write(f"{chave}: {valor if valor else '-'}\n")
            f.write("\n")


def gerar_dados_teste() -> List[Dict[str, any]]:
    """Generate dummy data for offline testing purposes."""
    return [
        {
            "cnj": "0000000-00.2020.8.99.9999",
            "tribunal": "TJXX",
            "grau": "1",
            "classe": "Procedimento Comum",
            "assunto": "Contrato",
            "orgao": "Vara Única",
            "status": "Em andamento",
            "partes": "AUTOR: Empresa A; RÉU: Empresa B",
            "dt_distribuicao": "2020-01-01",
            "qtd_movimentos": 10,
            "prazos_relevantes": "2020-02-01T00:00:00Z: Intimação para manifestação",
            "resumo_decisao": "Decisão interlocutória proferida.",
            "fase_execucao": "Não",
        },
        {
            "cnj": "1111111-11.2021.8.99.9999",
            "tribunal": "TJYY",
            "grau": "2",
            "classe": "Execução",
            "assunto": "Cobrança",
            "orgao": "Turma Recursal",
            "status": "Sentenciado",
            "partes": "EXEQUENTE: Fulano; EXECUTADO: Sicrano",
            "dt_distribuicao": "2021-05-10",
            "qtd_movimentos": 8,
            "prazos_relevantes": "",
            "resumo_decisao": "Sentença de mérito.",
            "fase_execucao": "Sim",
        },
    ]


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline DataJud para coleta e exportação de processos.")
    parser.add_argument("--nome", required=False, help="Nome da parte a pesquisar")
    parser.add_argument("--cnpj", required=False, help="Documento (CNPJ/CPF) da parte a pesquisar")
    parser.add_argument("--tribunais", nargs="*", default=[], help="Lista de tribunais (códigos) a consultar, ex: tjpe tjba trf5")
    parser.add_argument("--max-paginas", type=int, default=25, help="Máximo de páginas por tribunal (100 processos por página)")
    parser.add_argument("--desde", type=str, help="Data mínima de distribuição (YYYY-MM-DD)")
    parser.add_argument("--since-days", type=int, help="Número de dias para olhar para trás (desde hoje)")
    parser.add_argument("--excel", type=str, help="Caminho do arquivo Excel de saída")
    parser.add_argument("--csv", type=str, help="Caminho do arquivo CSV de saída")
    parser.add_argument("--pdf", type=str, help="Caminho do arquivo PDF de saída")
    parser.add_argument("--sqlite", type=str, help="Caminho do arquivo SQLite de saída")
    parser.add_argument("--no-pdf", action="store_true", help="Desativar a geração de PDF (mesmo se fpdf2 estiver instalado)")
    parser.add_argument("--selftest", action="store_true", help="Executar um teste interno sem chamadas à API (usa dados fictícios)")
    args = parser.parse_args(argv)

    # Determine date filter if provided
    desde: Optional[_dt.date] = None
    if args.desde:
        try:
            desde = _dt.date.fromisoformat(args.desde)
        except ValueError:
            print(f"[ERRO] Formato de data inválido em --desde: {args.desde}. Esperado YYYY-MM-DD.")
            sys.exit(1)
    elif args.since_days:
        desde = _dt.date.today() - _dt.timedelta(days=args.since_days)

    if args.selftest:
        dados = gerar_dados_teste()
    else:
        # Validate required parameters
        if not args.nome or not args.cnpj or not args.tribunais:
            parser.error("--nome, --cnpj e --tribunais são obrigatórios, exceto em --selftest")
        session = create_session()
        api_key = os.environ.get("DATAJUD_API_KEY", DEFAULT_API_KEY)
        session.headers.update({"Authorization": f"APIKey {api_key}"})
        dados: List[Dict[str, any]] = []
        for tribunal in args.tribunais:
            tribunal = tribunal.lower().strip()
            print(f"Consultando {tribunal.upper()}...")
            encontrados = buscar_processos(session, args.nome, args.cnpj, tribunal, args.max_paginas, desde)
            print(f"  > {len(encontrados)} processos encontrados")
            dados.extend(encontrados)
        # Deduplicate by (cnj, tribunal)
        df_all = pd.DataFrame(dados)
        if not df_all.empty:
            df_all.drop_duplicates(subset=["cnj", "tribunal"], inplace=True)
            dados = df_all.to_dict(orient="records")
        print(f"Total de processos/situações relevantes: {len(dados)}")

    # Export outputs
    if args.excel:
        exportar_excel(dados, args.excel)
        print(f"Arquivo Excel gerado em: {args.excel}")
    if args.csv:
        exportar_csv(dados, args.csv)
        print(f"Arquivo CSV gerado em: {args.csv}")
    if args.sqlite:
        exportar_sqlite(dados, args.sqlite)
        print(f"Banco de dados SQLite gerado em: {args.sqlite}")
    if args.pdf and not args.no_pdf:
        if _HAS_FPDF:
            try:
                exportar_pdf(dados, args.pdf)
                print(f"Arquivo PDF gerado em: {args.pdf}")
            except Exception as exc:
                print(f"[WARN] Falha ao gerar PDF: {exc}. Gerando relatório TXT como fallback...")
                fallback_path = os.path.splitext(args.pdf)[0] + ".txt"
                exportar_txt(dados, fallback_path)
                print(f"Arquivo TXT gerado em: {fallback_path}")
        else:
            print("[WARN] fpdf2 não está instalado. Gerando relatório TXT no lugar do PDF...")
            fallback_path = os.path.splitext(args.pdf)[0] + ".txt"
            exportar_txt(dados, fallback_path)
            print(f"Arquivo TXT gerado em: {fallback_path}")

    if not any([args.excel, args.csv, args.sqlite, args.pdf]):
        # If no export option was specified, print a summary to stdout
        for proc in dados:
            print(json.dumps(proc, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
