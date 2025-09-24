# -*- coding: utf-8 -*-
# pages/03_compras_entradas.py — Registrar compras/entradas de estoque + Telegram + Fracionamento + Edição/Exclusão
import json, unicodedata, re, time
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date, datetime

st.set_page_config(page_title="Compras / Entradas", page_icon="🧾", layout="wide")
st.title("🧾 Compras / Entradas de Estoque")

# ---------------- Utils de refresh/cache ----------------
def _refresh_now():
    st.session_state["_refresh_ts"] = time.time()
    st.cache_data.clear()
    try: st.rerun()
    except Exception: st.experimental_rerun()

BUMP = st.session_state.get("_refresh_ts", 0)  # usado para invalidar cache

def _fmt_brl(v: float) -> str:
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _fmt_num(v: float, casas=3) -> str:
    return f"{float(v):.{casas}f}".replace(".", ",")

# ========= credenciais =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=120)
def _load_df(aba: str, _bump: float | None = None) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        if cur.empty or any(h not in cur.columns for h in headers):
            cols = list(dict.fromkeys(headers + cur.columns.tolist()))
            df_head = pd.DataFrame(columns=cols)
            ws.clear()
            set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        df_head = pd.DataFrame(columns=headers)
        set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws

def _append_row(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns: row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

def _to_float(x):
    if x is None or str(x).strip()=="": return ""
    s = str(x).strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except: return ""

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

# ========= Telegram =========
def _tg_enabled() -> bool:
    try: return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception: return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return token, chat_id

def _tg_send(msg: str):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id: return
    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=6)
    except Exception:
        pass

# ========= abas/headers =========
PRODUTOS_ABA = "Produtos"
COMPRAS_ABA  = "Compras"
VENDAS_ABA   = "Vendas"
AJUSTES_ABA  = "Ajustes"
MOVS_ABA     = "MovimentosEstoque"

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# ---------- Botão Atualizar ----------
c_at, _ = st.columns([1, 6])
with c_at:
    if st.button("🔄 Atualizar dados", use_container_width=True, key=f"btn_atualizar_{BUMP}"):
        _refresh_now()

# ========= dados base =========
try:
    prod_df = _load_df(PRODUTOS_ABA, BUMP)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"): st.code(str(e))
    st.stop()

def _pick_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

COL = {
    "id":   _pick_col(prod_df, ["ID","Id","id","Codigo","Código","SKU"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "forn": _pick_col(prod_df, ["Fornecedor","FornecedorNome"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid","Und"]),
}

# ============================================================
# [.. restante do código de Entrada de Compras e Fracionamento ..]
# (mantém igual ao que já te enviei antes — sem alterações)
# ============================================================

# =========================================================
# 🚪 Navegação segura (sem PageNotFound)
# =========================================================
st.divider()
c_nav1, c_nav2 = st.columns(2)

def _try_switch(candidates: list[str]) -> bool:
    for cand in candidates:
        try:
            st.switch_page(cand)
            return True
        except Exception:
            pass
    return False

with c_nav1:
    if st.button("↩️ Voltar ao Cadastro/Editar", use_container_width=True):
        ok = _try_switch([
            "pages/02_cadastrar_produto.py",
            "pages/02_Cadastrar_Produto.py",
            "02_cadastrar_produto.py",
        ])
        if not ok:
            st.warning("Página de cadastro/edição não encontrada. Verifique o nome do arquivo em /pages.")

with c_nav2:
    if st.button("📦 Ir ao Catálogo", use_container_width=True):
        ok = _try_switch([
            "pages/01_produtos.py",
            "pages/01_Produtos.py",
            "01_produtos.py",
        ])
        if not ok:
            st.warning("Página do catálogo não encontrada. Confirme o nome do arquivo em /pages.")

# =========================================================
# ✏️ Editar / 🗑️ Apagar registros
# =========================================================
# (mantém a seção de edição/exclusão que já adaptei antes)
# =========================================================
