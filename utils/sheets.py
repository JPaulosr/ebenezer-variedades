# utils/sheets.py — camada única compartilhada para todo o app Ebenezér
# -*- coding: utf-8 -*-
"""
Importar em qualquer página assim:
    from utils.sheets import sheet, carregar_aba, append_rows, to_num, brl, safe_cost, first_col
"""
from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, date
from typing import Optional

import gspread
import pandas as pd
import streamlit as st
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────────────────────
#  NOMES DAS ABAS  (fonte única de verdade)
# ─────────────────────────────────────────────────────────────
ABA_PROD   = "Produtos"
ABA_VEND   = "Vendas"
ABA_COMP   = "Compras"
ABA_MOVS   = "MovimentosEstoque"
ABA_CLIEN  = "Clientes"
ABA_FIADO  = "Fiado"
ABA_FPAGT  = "Fiado_Pagamentos"

# Cabeçalhos esperados por aba
COLS = {
    ABA_PROD:  ["ID","Nome","Categoria","Unidade","Fornecedor","PreçoVenda",
                "EstoqueMin","LeadTimeDias","Ativo?","EstoqueCalc","CustoMedio","Foto","CustoAtual"],
    ABA_VEND:  ["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha",
                "FormaPagto","Obs","Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"],
    ABA_COMP:  ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total",
                "IDProduto","Obs","NF/Ref","ID","CustoUnit","FreteRateado","OutrosCustos","RefID"],
    ABA_MOVS:  ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID",
                "Documento/NF","Origem","SaldoApós"],
    ABA_CLIEN: ["Cliente","Telefone","Obs"],
    ABA_FIADO: ["ID","Data","Cliente","Valor","Vencimento","Status","Obs",
                "DataPagamento","FormaPagamento","ValorPago"],
    ABA_FPAGT: ["PagamentoID","DataPagamento","Cliente","Forma","TotalPago","IDsFiado","Obs"],
}

# Limite defensivo de custo unitário (valores acima disso são seriais de data bugados)
MAX_CUSTO_RAZOAVEL = 5_000.0


# ─────────────────────────────────────────────────────────────
#  CONEXÃO  (cache_resource = uma conexão por processo)
# ─────────────────────────────────────────────────────────────
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    return "".join(ch for ch in key
                   if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))


def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente nos Secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(str(svc["private_key"]))
    return svc


@st.cache_resource
def sheet() -> gspread.Spreadsheet:
    """Retorna o objeto Spreadsheet (uma conexão compartilhada para todo o app)."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url = st.secrets.get("PLANILHA_URL", "")
    if not url:
        st.error("🛑 PLANILHA_URL ausente nos Secrets."); st.stop()
    return gc.open_by_url(url) if str(url).startswith("http") else gc.open_by_key(url)


# ─────────────────────────────────────────────────────────────
#  LEITURA  (cache_data com TTL curto)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def carregar_aba(nome: str) -> pd.DataFrame:
    """
    Lê uma aba do Sheets e devolve DataFrame limpo.
    - Todas as colunas como string
    - Sem linhas totalmente vazias
    - Sem duplicatas de produto na aba Produtos (drop_duplicates por ID)
    """
    try:
        ws = sheet().worksheet(nome)
        df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
        df.columns = [c.strip() for c in df.columns]
        df = df.fillna("")

        # Proteção extra: remove duplicatas de produto (evita o bug de set_with_dataframe duplo)
        if nome == ABA_PROD and "ID" in df.columns:
            df = df.drop_duplicates(subset=["ID"], keep="first").reset_index(drop=True)

        return df
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Não foi possível carregar aba '{nome}': {e}")
        return pd.DataFrame()


def garantir_aba(nome: str, colunas: Optional[list] = None) -> gspread.Worksheet:
    """
    Retorna o worksheet, criando-o se não existir.
    Garante que os cabeçalhos esperados estejam presentes.
    """
    colunas = colunas or COLS.get(nome, [])
    sh = sheet()
    try:
        ws = sh.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10, len(colunas)))
        if colunas:
            ws.update("A1", [colunas])
        return ws

    # Garante cabeçalhos sem destruir dados existentes
    if colunas:
        hdrs = [h.strip() for h in (ws.row_values(1) or [])]
        faltando = [c for c in colunas if c not in hdrs]
        if faltando:
            ws.update("A1", [hdrs + faltando])

    return ws


# ─────────────────────────────────────────────────────────────
#  ESCRITA SEGURA  (append_rows — nunca apaga a aba inteira)
# ─────────────────────────────────────────────────────────────
def append_rows(ws: gspread.Worksheet, rows: list[dict]) -> None:
    """
    Acrescenta linhas ao final da aba usando append_rows da API.
    NUNCA usa ws.clear() + set_with_dataframe (que causava duplicatas e perda de dados).
    """
    if not rows:
        return
    hdrs = [h.strip() for h in ws.row_values(1)]
    data = [[row.get(h, "") for h in hdrs] for row in rows]
    ws.append_rows(data, value_input_option="USER_ENTERED")


def gerar_id(prefixo: str = "ID") -> str:
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"


# ─────────────────────────────────────────────────────────────
#  CONVERSÃO NUMÉRICA
# ─────────────────────────────────────────────────────────────
def to_num(x, default: float = 0.0) -> float:
    """
    Converte qualquer valor para float.
    Aceita: vírgula decimal (BR), ponto decimal (US), R$, parênteses negativos.
    Rejeita silenciosamente strings vazias e NaN → retorna default.
    """
    if x is None:
        return default
    if isinstance(x, (int, float)):
        import math
        return default if math.isnan(x) else float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", ""):
        return default
    s = s.replace("−", "-")
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    s = s.replace("R$", "").replace(" ", "").replace("\u00A0", "")
    # Formato BR: tem vírgula → remove pontos de milhar, troca vírgula por ponto
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count("-") > 1:
        s = "-" + s.replace("-", "")
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        v = float(s)
    except ValueError:
        return default
    return -abs(v) if neg else v


def safe_cost(x, max_val: float = MAX_CUSTO_RAZOAVEL) -> float:
    """
    Converte para float E descarta valores absurdos (seriais de data do Excel/Sheets).
    Valores > max_val são tratados como zero (dado inválido).
    """
    v = to_num(x)
    return 0.0 if v > max_val or v < 0 else v


def brl(v) -> str:
    """Formata valor como moeda brasileira: R$ 1.234,56"""
    try:
        f = float(v)
        s = f"{abs(f):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return ("-R$ " if f < 0 else "R$ ") + s
    except Exception:
        return "R$ 0,00"


def fmt_num(v) -> str:
    """Formata número sem casas decimais desnecessárias: 3.0 → '3', 3.5 → '3,5'"""
    try:
        f = float(v)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return f"{f:.2f}".replace(".", ",").rstrip("0").rstrip(",")
    except Exception:
        return str(v)


# ─────────────────────────────────────────────────────────────
#  UTILIDADES DE DATAFRAME
# ─────────────────────────────────────────────────────────────
def first_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Retorna o nome da primeira coluna do df que estiver na lista candidates."""
    if df is None or df.empty:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def strip_acc(s: str) -> str:
    """Remove acentos de uma string."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", str(s or ""))
        if unicodedata.category(ch) != "Mn"
    )


def norm_str(s: str) -> str:
    """Normaliza string para comparação: sem acentos, minúsculo, sem espaços extras."""
    return re.sub(r"\s+", " ", strip_acc(str(s or "")).lower()).strip()


def parse_date(s) -> Optional[date]:
    """Converte string de data para objeto date. Aceita DD/MM/YYYY e YYYY-MM-DD."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    txt = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            pass
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="coerce").date()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
#  ESTOQUE  (cálculo via MovimentosEstoque — fonte única)
# ─────────────────────────────────────────────────────────────
def norm_tipo_mov(t: str) -> str:
    """Classifica um tipo de movimento em: entrada | saida | ajuste | outro."""
    raw = str(t or "")
    low = "".join(
        ch for ch in unicodedata.normalize("NFKD", raw.lower())
        if unicodedata.category(ch) != "Mn"
    )
    if "fracion" in low:
        return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = re.sub(r"[^a-z]", "", low)
    if "contagem" in lowc or "inventario" in lowc:
        return "ajuste"
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc:
        return "entrada"
    if "saida" in lowc or "venda" in lowc or "baixa" in lowc:
        return "saida"
    if "ajuste" in lowc:
        return "ajuste"
    return "outro"


def calcular_estoque(df_mov: pd.DataFrame) -> dict[str, float]:
    """
    Recebe a aba MovimentosEstoque e devolve {IDProduto: saldo_atual}.
    Fonte única de verdade para estoque em todo o app.
    """
    if df_mov.empty:
        return {}

    c_pid  = first_col(df_mov, ["IDProduto", "ProdutoID", "ID"])
    c_qtd  = first_col(df_mov, ["Qtd", "Quantidade"])
    c_tipo = first_col(df_mov, ["Tipo", "tipo"])

    if not (c_pid and c_qtd and c_tipo):
        return {}

    saldo: dict[str, float] = {}
    for _, r in df_mov.iterrows():
        pid  = str(r.get(c_pid, "")).strip()
        if not pid:
            continue
        tipo = norm_tipo_mov(r.get(c_tipo, ""))
        qtd  = to_num(r.get(c_qtd, 0))
        cur  = saldo.get(pid, 0.0)
        if tipo == "entrada":
            saldo[pid] = cur + qtd
        elif tipo == "saida":
            saldo[pid] = cur - qtd
        elif tipo == "ajuste":
            saldo[pid] = cur + qtd
    return saldo


# ─────────────────────────────────────────────────────────────
#  TELEGRAM  (centralizado)
# ─────────────────────────────────────────────────────────────
def tg_send(msg: str) -> None:
    """Envia mensagem via Telegram se TELEGRAM_ENABLED == '1'."""
    try:
        if str(st.secrets.get("TELEGRAM_ENABLED", "0")) != "1":
            return
        token   = str(st.secrets.get("TELEGRAM_TOKEN", ""))
        chat_id = str(st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "")
                      or st.secrets.get("TELEGRAM_CHAT_ID", ""))
        if not token or not chat_id:
            return
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=8,
        )
    except Exception:
        pass


def tg_media(media: list[dict]) -> None:
    """Envia grupo de mídias via Telegram."""
    try:
        if str(st.secrets.get("TELEGRAM_ENABLED", "0")) != "1":
            return
        token   = str(st.secrets.get("TELEGRAM_TOKEN", ""))
        chat_id = str(st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "")
                      or st.secrets.get("TELEGRAM_CHAT_ID", ""))
        if not token or not chat_id or not media:
            return
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMediaGroup",
            json={"chat_id": chat_id, "media": media[:10]},
            timeout=12,
        )
    except Exception:
        pass
