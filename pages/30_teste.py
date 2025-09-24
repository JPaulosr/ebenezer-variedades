# -*- coding: utf-8 -*-
# pages/04_estoque.py — Estoque (MovimentosEstoque como fonte única) + busca + auto-refresh (UI moderna com cards)

import json, unicodedata as _ud, re
from datetime import date, datetime

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from pathlib import Path

# =========================
# UI BASE / TEMA
# =========================
st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📦", layout="wide")

st.markdown("""
<style>
:root{
  --bg: rgba(255,255,255,.03);
  --bg2: rgba(255,255,255,.06);
  --borda: rgba(255,255,255,.12);
  --muted: rgba(255,255,255,.65);
  --ok: #22c55e; --warn:#f59e0b; --err:#ef4444;
}
.block-container { padding-top: 1.2rem; }
.kpi{border:1px solid var(--borda); background:var(--bg); padding:1rem 1.1rem; border-radius:16px;}
.kpi h3{margin:.2rem 0 .6rem 0; font-size:1.05rem; color:var(--muted); font-weight:600}
.kpi .big{font-size:1.8rem; font-weight:800; line-height:1.1}
.kpi .sub{font-size:.9rem; color:var(--muted)}
.card{border:1px solid var(--borda); background:var(--bg); padding:1rem; border-radius:16px; margin:.4rem 0 1rem 0;}
.card h3{margin:0 0 .6rem 0}
.badge{display:inline-block; padding:.15rem .5rem; border-radius:999px; border:1px solid var(--borda); background:var(--bg2); font-size:.78rem; color:var(--muted)}
.small{color:var(--muted); font-size:.86rem}
</style>
""", unsafe_allow_html=True)

# ---------- refresh automático ----------
if st.session_state.pop("_first_load_estoque", True):
    st.cache_data.clear()
st.session_state.setdefault("_first_load_estoque", False)

# =========================
# Credenciais / Conexão
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
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
        st.error("🛑 Segredo PLANILHA_URL ausente.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_resource
def _sheet_titles() -> set[str]:
    try:
        return {ws.title for ws in _sheet().worksheets()}
    except Exception:
        return set()

@st.cache_data(ttl=1, show_spinner=False)
def _load_df(aba: str) -> pd.DataFrame:
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
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0).fillna("")
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# =========================
# Utilidades
# =========================
def _to_num(x) -> float:
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−","-")
    neg_paren = s.startswith("(") and s.endswith(")")
    if neg_paren: s = s[1:-1]
    s = s.replace("R$","").replace(" ","")
    if "," in s: s = s.replace(".","").replace(",",".")
    s = re.sub(r"(?<!^)-","",s)
    s = re.sub(r"[^0-9.\-]","",s)
    try: v=float(s)
    except: v=0.0
    if neg_paren: v=-abs(v)
    return v

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s=str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _strip_accents_low(s: str) -> str:
    s=_ud.normalize("NFKD", str(s or ""))
    s="".join(ch for ch in s if _ud.category(ch)!="Mn")
    return s.lower().strip()

def _norm_tipo(t: str) -> str:
    raw=str(t or ""); low=_strip_accents_low(raw)
    if "fracion" in low:
        if "+" in raw: return "entrada"
        if "-" in raw: return "saida"
        return "outro"
    low_clean=re.sub(r"[^a-z]","",low)
    if "entrada" in low_clean or "compra" in low_clean or "estorno" in low_clean: return "entrada"
    if "saida" in low_clean or "venda" in low_clean or "baixa" in low_clean: return "saida"
    if "ajuste" in low_clean: return "ajuste"
    return "outro"

def _prod_key_from(prod_id, prod_nome):
    pid=_nz(prod_id)
    return pid if pid else f"nm:{_strip_accents_low(_nz(prod_nome))}"

# =========================
# Abas & Headers
# =========================
ABA_PRODUTOS="Produtos"
ABA_COMPRAS="Compras"
ABA_MOV="MovimentosEstoque"
ABA_VENDAS="Vendas"

COMPRAS_HEADERS=["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS=["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# =========================
# Carregar bases
# =========================
titles=_sheet_titles()
prod_df=_load_df(ABA_PRODUTOS)
compras_df=_load_df(ABA_COMPRAS) if ABA_COMPRAS in titles else pd.DataFrame(columns=COMPRAS_HEADERS)
mov_df=_load_df(ABA_MOV) if ABA_MOV in titles else pd.DataFrame(columns=MOV_HEADERS)

# =========================
# Normalizações
# =========================
COLP={"id":next((c for c in ["ID","Id","id","Codigo","Código","SKU"] if c in prod_df.columns),None),
      "nome":next((c for c in ["Nome","Produto","Descrição","Descricao"] if c in prod_df.columns),None)}
if COLP["nome"] is None:
    st.error("Aba **Produtos** precisa ter coluna de nome.")
    st.stop()

base=prod_df.copy()
base["__key"]=base.apply(lambda r:_prod_key_from(r.get(COLP["id"],""), r.get(COLP["nome"],"")),axis=1)
base["Produto"]=base[COLP["nome"]]
base["IDProduto"]=base[COLP["id"]] if COLP["id"] else ""

for c in COMPRAS_HEADERS:
    if c not in compras_df.columns: compras_df[c]=""
if not compras_df.empty:
    compras_df["__key"]=compras_df.apply(lambda r:_prod_key_from(r.get("IDProduto",""), r.get("Produto","")),axis=1)
    compras_df["Custo_num"]=compras_df["Custo Unitário"].apply(_to_num)
    last_cost=compras_df.groupby("__key",as_index=False).tail(1)
    custo_atual_map=dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else: custo_atual_map={}

for c in MOV_HEADERS:
    if c not in mov_df.columns: mov_df[c]=""
if not mov_df.empty:
    mov_df["Tipo_norm"]=mov_df["Tipo"].apply(_norm_tipo)
    mov_df["Qtd_num"]=mov_df["Qtd"].apply(_to_num)
    mov_df["__key"]=mov_df.apply(lambda r:_prod_key_from(r.get("IDProduto",""), r.get("Produto","")),axis=1)
    def _sum_mov(tipo):
        m=mov_df[mov_df["Tipo_norm"]==tipo]
        return {} if m.empty else m.groupby("__key")["Qtd_num"].sum().to_dict()
    entradas_mov=_sum_mov("entrada"); saidas_mov=_sum_mov("saida"); ajustes_mov=_sum_mov("ajuste")
else: entradas_mov,saidas_mov,ajustes_mov={},{},{}

# =========================
# Consolidação
# =========================
df=base[["__key","Produto","IDProduto"]].copy()
def _get(mapper,key): return float(mapper.get(key,0.0))
df["Entradas"]=df["__key"].apply(lambda k:_get(entradas_mov,k))
df["Saidas"]=df["__key"].apply(lambda k:_get(saidas_mov,k))
df["Ajustes"]=df["__key"].apply(lambda k:_get(ajustes_mov,k))
df["EstoqueAtual"]=df["Entradas"]-df["Saidas"]+df["Ajustes"]
df["CustoAtual"]=df["__key"].apply(lambda k:float(custo_atual_map.get(k,0.0)))
df["ValorTotal"]=(df["EstoqueAtual"].astype(float)*df["CustoAtual"].astype(float)).round(2)

# =========================
# HEADER
# =========================
left,right=st.columns([0.7,0.3])
with left:
    st.markdown("<h1>📦 Estoque — Movimentos & Ajustes</h1>",unsafe_allow_html=True)
    st.markdown(f"<div class='small'>Fonte única: <b>{ABA_MOV}</b> • Atualizado: <code>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</code></div>",unsafe_allow_html=True)
with right:
    if Path("pages/03_Compras_Produtos_Entradas.py").exists():
        st.page_link("pages/03_Compras_Produtos_Entradas.py", label="🧾 Registrar Compras / Entradas", icon="🧾")
    if Path("pages/01_Produtos.py").exists():
        st.page_link("pages/01_Produtos.py", label="📦 Ir ao Catálogo", icon="📦")

# resto igual (busca, cards, tabelas, forms)...
