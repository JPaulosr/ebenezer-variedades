# pages/04_vendas_rapidas.py — Vendas rápidas (carrinho + histórico/estorno/duplicar) + Telegram
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date, timedelta
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # para Telegram

st.set_page_config(page_title="Vendas rápidas", page_icon="🧾", layout="wide")
st.title("🧾 Vendas rápidas (carrinho)")

# ================= Helpers =================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, candidates) -> str | None:
    if df is None or df.empty: return None
    for c in candidates:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _to_num(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    s = s.replace(".", "").replace(",", ".") if s.count(",")==1 and s.count(".")>1 else s.replace(",", ".")
    try: return float(s)
    except: return 0.0

def _fmt_brl_num(v):
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _gerar_id(prefixo="F"):
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _garantir_aba(sh, nome, cols):
    try:
        ws = sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10,len(cols)))
        ws.update("A1", [cols])
        return ws
    headers = ws.row_values(1) or []
    headers = [h.strip() for h in headers]
    falt = [c for c in cols if c not in headers]
    if falt:
        ws.update("A1", [headers + falt])
    return ws

def _append_rows(ws, rows: list[dict]):
    headers = ws.row_values(1)
    hdr = [h.strip() for h in headers]
    to_append = []
    for d in rows:
        to_append.append([d.get(h, "") for h in hdr])
    if to_append:
        ws.append_rows(to_append, value_input_option="USER_ENTERED")

# -------- Telegram --------
def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return token, chat_id

def _tg_send(msg: str):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id: return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=6)
    except Exception:
        pass

# helper para exibir linhas de item (corrige KeyError)
def _render_item_line(x: dict) -> str:
    pid   = x.get("IDProduto") or x.get("ProdutoID") or x.get("ID") or x.get("id") or "?"
    qtd   = int(_to_num(x.get("Qtd") if "Qtd" in x else x.get("qtd", 1)))
    preco = _to_num(x.get("PrecoUnit") if "PrecoUnit" in x else x.get("preco", 0))
    return f"• {pid} x{qtd} @ {_fmt_brl_num(preco)}"
# ---------------------------

# ================= Abas principais =================
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_FIADO = "Fiado"
ABA_CLIENTES = "Clientes"

COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_CLIENTES = ["Cliente","Telefone","Obs"]

# ---- Clientes (cadastro) ----
def _carregar_clientes() -> list[str]:
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        if dfc.empty: return []
        col_cli = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        return sorted(list(dict.fromkeys([str(x).strip() for x in dfc[col_cli].dropna()])))
    except Exception:
        return []

# =============== Catálogo ===============
try:
    dfp = carregar_aba(ABA_PROD)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"): st.code(str(e))
    st.stop()

col_id   = _first_col(dfp, ["ID","Codigo","Código","SKU"])
col_nome = _first_col(dfp, ["Nome","Produto","Descrição"])
col_preco= _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"])
col_unid = _first_col(dfp, ["Unidade","Und"])
if not col_id or not col_nome:
    st.error("A aba Produtos precisa ter colunas de ID e Nome."); st.stop()

dfp["_label"] = dfp.apply(lambda r: f"{str(r[col_id])} — {str(r[col_nome])}", axis=1)
cat_map = dfp.set_index("_label")[[col_id, col_nome, col_preco, col_unid]].to_dict("index")
labels = ["(selecione)"] + sorted(cat_map.keys())

# ================= Estado inicial =================
if "cart" not in st.session_state: st.session_state["cart"] = []
if "forma" not in st.session_state: st.session_state["forma"] = "Dinheiro"
if "obs" not in st.session_state:   st.session_state["obs"] = ""
if "data_venda" not in st.session_state: st.session_state["data_venda"] = date.today()
if "desc" not in st.session_state:  st.session_state["desc"] = 0.0
if "cliente" not in st.session_state: st.session_state["cliente"] = ""
if "venc_fiado" not in st.session_state: st.session_state["venc_fiado"] = date.today() + timedelta(days=30)

# =============== Carrinho UI (omitido p/ brevidade) ===============
# ... [mesmo fluxo de adicionar/remover itens do carrinho]

# No trecho de REGISTRAR VENDA, substitua a parte do Telegram:
# itens_txt = "\n".join([f"• {x['id']} ..." for x in novas])
# >>> por:
itens_txt = "\n".join(_render_item_line(x) for x in novas)
# ---------------------------------

# No trecho de ESTORNO, se quiser listar itens:
# itens_txt_estorno = "\n".join(_render_item_line(x) for x in novas)
# e incluir no msg do Telegram se desejar
