# pages/31_teste2.py
# -*- coding: utf-8 -*-
import json, unicodedata, re
from datetime import date
from collections.abc import Mapping

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Teste — Produtos (normalização robusta)", page_icon="🧪", layout="wide")
st.title("🧪 Teste — Normalização da aba Produtos")

# =========================
# Helpers de conexão (iguais ao app)
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    if not isinstance(svc, Mapping):
        st.error("🛑 GCP_SERVICE_ACCOUNT inválido."); st.stop()
    pk = str(svc.get("private_key",""))
    if "BEGIN PRIVATE KEY" not in pk:
        st.error("🛑 private_key inválida. Cole a chave completa (BEGIN/END)."); st.stop()
    svc = {**svc, "private_key": _normalize_private_key(pk)}
    return svc

@st.cache_resource
def _connect():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc     = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL não está no Secrets."); st.stop()
    ss = gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)
    return ss

@st.cache_data(ttl=20, show_spinner=False)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = _connect().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    df = df.dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

# =========================
# Utils
# =========================
def _canon_id(x) -> str:
    return re.sub(r"[^0-9]", "", str(x or ""))

def _collapse_duplicate_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Se após renomear ficaram colunas com o MESMO nome, une linha a linha (1º valor não-vazio) e remove as extras."""
    if not df.columns.duplicated().any():
        return df
    # nomes duplicados
    for name in pd.unique(df.columns[df.columns.duplicated()]):
        bloco = df.loc[:, df.columns == name]
        # trata "", " " como nulos
        bloco2 = bloco.replace({"": pd.NA, " ": pd.NA})
        # pega o 1º valor não-nulo por linha
        col_unica = bloco2.bfill(axis=1).iloc[:, 0]
        # remove TODAS as colunas duplicadas desse nome e reatribui apenas uma
        df.drop(columns=bloco.columns, inplace=True)
        df[name] = col_unica
    return df

def _ensure_series_numeric(df: pd.DataFrame, colname: str) -> pd.Series:
    """Garante que df[colname] seja Series 1-D antes de to_numeric."""
    obj = df[colname]
    if isinstance(obj, pd.DataFrame):
        obj = obj.replace({"": pd.NA, " ": pd.NA}).bfill(axis=1).iloc[:, 0]
    return pd.to_numeric(obj, errors="coerce")

def _fmt_brl(v):
    try:
        return ("R$ " + f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except:
        return "R$ 0,00"

# =========================
# Carregar e normalizar Produtos
# =========================
prod = carregar_aba("Produtos")

if prod.empty:
    st.warning("Aba Produtos está vazia.")
    st.stop()

# 1) Mapeamento unificado de nomes (custo/preço)
ren = {
    "ID":"ID","Nome":"Nome","Categoria":"Categoria","Unidade":"Unidade","Fornecedor":"Fornecedor",

    # CUSTO → CustoAtual
    "CustoAtual":"CustoAtual",
    "CustoMedio":"CustoAtual",
    "Custo Médio":"CustoAtual",
    "Custo":"CustoAtual",

    # PREÇO → PrecoVenda
    "PreçoVenda":"PrecoVenda",
    "Preço Venda":"PrecoVenda",
    "PrecoVenda":"PrecoVenda",
    "Preço":"PrecoVenda",

    "Markup %":"MarkupPct","Margem %":"MargemPct",
    "EstoqueAtual":"EstoqueAtual","EstoqueMin":"EstoqueMin","LeadTimeDias":"LeadTimeDias","Ativo?":"Ativo"
}

for k, v in ren.items():
    if k in prod.columns and v != k:
        prod.rename(columns={k: v}, inplace=True)

# 2) Colapsa colunas com nomes duplicados geradas pelo rename
prod = _collapse_duplicate_name_columns(prod)

# 3) Garante colunas essenciais
needed = ["ID","Nome","Categoria","Fornecedor","EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda","Ativo"]
for c in needed:
    if c not in prod.columns:
        prod[c] = pd.NA

# 4) Converte numéricas com segurança (mesmo se viessem 2-D)
for c in ["EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda"]:
    prod[c] = _ensure_series_numeric(prod, c)

# 5) Derivadas
prod["KeyID"] = prod["ID"].apply(_canon_id)
prod["ValorEstoque"] = prod["CustoAtual"].fillna(0) * prod["EstoqueAtual"].fillna(0)

# =========================
# UI — checagem rápida
# =========================
c1, c2, c3 = st.columns(3)
c1.metric("Produtos", f"{len(prod)}")
c2.metric("Itens com custo", f"{int((prod['CustoAtual'].fillna(0)>0).sum())}")
c3.metric("💰 Valor em estoque (aprox.)", _fmt_brl(float(prod["ValorEstoque"].fillna(0).sum())))

st.caption(f"Hoje: {date.today().strftime('%d/%m/%Y')}  •  Colunas unificadas: Custo→CustoAtual, Preço→PrecoVenda")

st.subheader("Amostra")
cols_show = [c for c in ["ID","Nome","Categoria","Fornecedor","CustoAtual","PrecoVenda","EstoqueAtual","EstoqueMin","ValorEstoque","Ativo"] if c in prod.columns]
st.dataframe(prod[cols_show] if cols_show else prod, use_container_width=True, hide_index=True)
