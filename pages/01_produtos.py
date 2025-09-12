# pages/01_produtos.py — Catálogo de Produtos (estoque via MovimentosEstoque)
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos — Catálogo & Busca")

# Auto-refresh sinalizado por outras páginas
if st.session_state.pop("_force_refresh", False):
    st.cache_data.clear()
    st.rerun()

# =========================
# Utils
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10, show_spinner=False)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _first_col(df: pd.DataFrame, cands: list[str]) -> str | None:
    for c in cands:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower: return lower[c.lower()]
    return None

def _to_num(x) -> float:
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("R$","").replace(" ","")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if s.count(".") > 1:
        p = s.split("."); s = "".join(p[:-1]) + "." + p[-1]
    try: return float(s)
    except: return 0.0

def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower().strip()

def _norm_tipo(t: str) -> str:
    """
    'entrada'  (compra, estorno, fracionamento +)
    'saida'    (venda, baixa, fracionamento -)
    'ajuste'   (ajuste)
    """
    raw = str(t or "")
    low = _strip_accents_low(raw)
    if "fracion" in low:
        if "+" in raw: return "entrada"
        if "-" in raw: return "saida"
        return "outro"
    lowc = re.sub(r"[^a-z]", "", low)
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
    if "ajuste"  in lowc: return "ajuste"
    return "outro"

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _prod_key_from(prod_id, prod_nome):
    pid = _nz(prod_id)
    if pid: return pid
    return f"nm:{_strip_accents_low(_nz(prod_nome))}"

# =========================
# Abas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"   # fonte única de quantidades
ABA_COMPRAS  = "Compras"             # custo

# =========================
# Carregamento
# =========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos."); st.code(str(e)); st.stop()

try:
    df_mov  = carregar_aba(ABA_MOV)
except Exception:
    df_mov  = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

try:
    df_comp = carregar_aba(ABA_COMPRAS)
except Exception:
    df_comp = pd.DataFrame(columns=["IDProduto","Qtd","Custo Unitário"])

# =========================
# Colunas importantes
# =========================
col_id   = _first_col(df_prod, ["ID","Id","Codigo","Código","SKU"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat  = _first_col(df_prod, ["Categoria"])
col_forn = _first_col(df_prod, ["Fornecedor"])
col_estq_min = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo","EstqMin"])
col_preco = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço","Preco"])

if not col_nome:
    st.error("Aba **Produtos** precisa ter uma coluna de nome (ex.: Nome/Produto)."); st.stop()

# =========================
# Base Produtos
# =========================
base = df_prod.copy()
base["__key"] = base.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
base["IDProduto"] = base[col_id] if col_id else ""
base["Produto"]   = base[col_nome]

# =========================
# Movimentos → Entradas/Saídas/Ajustes (fonte única)
# =========================
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["Tipo_norm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["Qtd_num"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]     = df_mov.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)

    def _sum_mov(tipo):
        m = df_mov[df_mov["Tipo_norm"] == tipo]
        if m.empty: return {}
        return m.groupby("__key")["Qtd_num"].sum().to_dict()

    entradas_mov = _sum_mov("entrada")
    saidas_mov   = _sum_mov("saida")
    ajustes_mov  = _sum_mov("ajuste")
else:
    entradas_mov, saidas_mov, ajustes_mov = {}, {}, {}

# =========================
# Compras → custo atual (última compra)
# =========================
col_comp_id = _first_col(df_comp, ["IDProduto","ProdutoID","ID"])
col_comp_cu = _first_col(df_comp, ["Custo Unitário","CustoUnitário","Custo Unit","Custo"])
if not df_comp.empty and col_comp_id and col_comp_cu:
    df_comp["__key"] = df_comp.apply(lambda r: _prod_key_from(r.get(col_comp_id,""), r.get("Produto","")), axis=1)
    df_comp["Custo_num"] = df_comp[col_comp_cu].map(_to_num)
    last_cost = df_comp.groupby("__key", as_index=False).tail(1)
    custo_atual_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else:
    custo_atual_map = {}

# =========================
# Consolidação
# =========================
df = base[["__key","IDProduto","Produto"]].copy()
def _get(m, k): return float(m.get(k, 0.0))

df["Entradas"] = df["__key"].apply(lambda k: _get(entradas_mov, k))
df["Saidas"]   = df["__key"].apply(lambda k: _get(saidas_mov,   k))
df["Ajustes"]  = df["__key"].apply(lambda k: _get(ajustes_mov,  k))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df["ValorTotal"]   = (df["EstoqueAtual"] * df["CustoAtual"]).round(2)

# =========================
# Filtros
# =========================
top, mid = st.columns([2.5, 1.5])
with top:
    termo = st.text_input("🔎 Buscar", placeholder="ID, nome, fornecedor, categoria...").strip()
with mid:
    only_low = st.checkbox("⚠️ Somente baixo estoque", value=False,
                           help="Itens com EstoqueAtual ≤ EstoqueMin (se existir a coluna).")

c1, c2 = st.columns(2)
with c1:
    if col_cat and col_cat in df_prod.columns:
        cats = ["(todas)"] + sorted(pd.Series(df_prod[col_cat].dropna().astype(str).unique()).tolist())
        cat = st.selectbox("Categoria", cats)
    else:
        cat = "(todas)"
with c2:
    if col_forn and col_forn in df_prod.columns:
        forns = ["(todos)"] + sorted(pd.Series(df_prod[col_forn].dropna().astype(str).unique()).tolist())
        forn = st.selectbox("Fornecedor", forns)
    else:
        forn = "(todos)"

# junta info de categoria/fornecedor para filtro (sem poluir a saída)
df = df.merge(df_prod[[col_id, col_nome, col_cat, col_forn]] if col_id else df_prod[[col_nome, col_cat, col_forn]],
              left_on="IDProduto", right_on=col_id if col_id else col_nome, how="left")

mask = pd.Series(True, index=df.index)
if termo:
    t = termo.lower()
    mask &= df.apply(lambda r: t in " ".join([str(x).lower() for x in [r.get("IDProduto",""), r.get("Produto",""), r.get(col_cat,""), r.get(col_forn,"")]]), axis=1)
if col_cat and cat != "(todas)" and col_cat in df.columns:
    mask &= (df[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)" and col_forn in df.columns:
    mask &= (df[col_forn].astype(str) == forn)

if only_low and col_estq_min and col_estq_min in df_prod.columns:
    # traz EstoqueMin para o df e filtra
    df = df.merge(df_prod[[col_id, col_estq_min]] if col_id else df_prod[[col_nome, col_estq_min]],
                  left_on="IDProduto", right_on=col_id if col_id else col_nome, how="left", suffixes=("","_x"))
    estq_min = df[col_estq_min].map(_to_num).fillna(0)
    mask &= (df["EstoqueAtual"] <= estq_min)

dfv = df[mask].copy()

# =========================
# Exibição
# =========================
cols_show = ["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
if col_cat and col_cat in dfv.columns: cols_show.insert(2, col_cat)
if col_forn and col_forn in dfv.columns: cols_show.insert(3, col_forn)
if col_estq_min and col_estq_min in df_prod.columns and col_estq_min in dfv.columns:
    if col_estq_min not in cols_show: cols_show.append(col_estq_min)

dfv = dfv.loc[:, [c for c in cols_show if c in dfv.columns]]
st.dataframe(dfv.sort_values("Produto"), use_container_width=True, hide_index=True)

st.caption("""
• **EstoqueAtual** = Entradas − Saídas ± Ajustes (a partir da aba **MovimentosEstoque**, incluindo Fracionamento).
• **CustoAtual** = último custo de compra.
• Use **Compras** / **Fracionar** / **Ajustes** para movimentar estoque.
""")
