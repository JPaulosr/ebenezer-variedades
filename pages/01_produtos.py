# pages/01_produtos.py — Catálogo de Produtos
# -*- coding: utf-8 -*-
import json, unicodedata
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos — Catálogo & Busca")

# -----------------------------
# Funções auxiliares
# -----------------------------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()

    # Se vier string (JSON bruto), converte
    if isinstance(svc, str):
        svc = json.loads(svc)

    # Garante cópia mutável
    svc = dict(svc)

    # Normaliza a chave privada
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def conectar_sheets():
    svc = _load_sa()
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)

    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()

    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = sh.worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    return df

# -----------------------------
# Principal
# -----------------------------
ABA = "Produtos"
try:
    df = carregar_aba(ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

df.columns = [c.strip() for c in df.columns]
col_cat  = "Categoria" if "Categoria" in df.columns else None
col_forn = "Fornecedor" if "Fornecedor" in df.columns else None

# Barra de busca e filtros
l, c, r = st.columns([2, 1.2, 1.2])
with l:
    termo = st.text_input("🔎 Buscar", placeholder="ID, nome, fornecedor, categoria...").strip()
with c:
    cat = st.selectbox("Categoria", ["(todas)"] + sorted(df[col_cat].dropna().astype(str).unique()) if col_cat else ["(todas)"])
with r:
    forn = st.selectbox("Fornecedor", ["(todos)"] + sorted(df[col_forn].dropna().astype(str).unique()) if col_forn else ["(todos)"])

# Aplicar filtros
mask = pd.Series(True, index=df.index)
if termo:
    t = termo.lower()
    mask &= df.apply(lambda r: t in " ".join([str(x).lower() for x in r.values]), axis=1)
if col_cat and cat != "(todas)":
    mask &= (df[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)":
    mask &= (df[col_forn].astype(str) == forn)

dfv = df[mask].reset_index(drop=True)

# Mostrar tabela final
st.dataframe(dfv, use_container_width=True, hide_index=True)

st.caption("Use a página **Dashboard** para ver KPIs, alertas e gráficos.")
