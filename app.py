# app.py — Dashboard Ebenezér Variedades
# -*- coding: utf-8 -*-
import json, unicodedata
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px

import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

# ------------------------
# CONFIG
# ------------------------
st.set_page_config(page_title="Ebenezér Variedades — Dashboard", page_icon="🧮", layout="wide")
st.title("🧮 Dashboard — Ebenezér Variedades")

# ------------------------
# HELPERS (auth + sheets)
# ------------------------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_service_account_from_secrets() -> Optional[dict]:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        return None
    if isinstance(svc, str):
        try:
            svc = json.loads(svc)
        except Exception as e:
            st.error("🛑 GCP_SERVICE_ACCOUNT é uma string inválida (JSON).")
            st.caption(str(e));  return None
    if not isinstance(svc, dict):
        st.error("🛑 GCP_SERVICE_ACCOUNT precisa ser um objeto JSON.");  return None

    required = ["type","project_id","private_key_id","private_key","client_email","token_uri"]
    missing = [k for k in required if k not in svc]
    if missing:
        st.error("🛑 Faltam campos no Service Account: " + ", ".join(missing));  return None

    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc

@st.cache_resource(show_spinner=True)
def conectar_sheets():
    svc = _load_service_account_from_secrets()
    if not svc:
        raise ValueError("Segredo GCP_SERVICE_ACCOUNT ausente/ inválido.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)

    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        raise ValueError("PLANILHA_URL não definido em Secrets.")
    if url_or_id.startswith("http"):
        return gc.open_by_url(url_or_id)
    else:
        return gc.open_by_key(url_or_id)

@st.cache_data(show_spinner=True)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nome_aba)
        df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
        df = df.dropna(how="all")
        return df
    except Exception as e:
        raise

# ------------------------
# CARREGAR PRODUTOS
# ------------------------
ABA_PRODUTOS = "Produtos"
try:
    df = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Não consegui abrir a planilha. Verifique Secrets e compartilhamento.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

if df.empty:
    st.warning(f"A aba **{ABA_PRODUTOS}** está vazia.")
    st.stop()

# normalizar nomes de colunas esperadas
df.columns = [c.strip() for c in df.columns]
m = {
    "ID":"ID", "Nome":"Nome", "Categoria":"Categoria", "Unidade":"Unidade",
    "Fornecedor":"Fornecedor", "CustoAtual":"CustoAtual", "PreçoVenda":"PrecoVenda",
    "Preço Venda":"PrecoVenda", "PrecoVenda":"PrecoVenda",
    "Markup %":"MarkupPct", "Margem %":"MargemPct",
    "EstoqueAtual":"EstoqueAtual", "EstoqueMin":"EstoqueMin",
    "LeadTimeDias":"LeadTimeDias", "Ativo?":"Ativo"
}
for k,v in m.items():
    if k in df.columns: df.rename(columns={k:v}, inplace=True)

# garantir colunas
for c in ["CustoAtual","PrecoVenda","MarkupPct","MargemPct","EstoqueAtual","EstoqueMin","LeadTimeDias"]:
    if c not in df.columns: df[c] = None
for c in ["Categoria","Fornecedor","Ativo","Nome"]:
    if c not in df.columns: df[c] = None

# tipos
num_cols = ["CustoAtual","PrecoVenda","MarkupPct","MargemPct","EstoqueAtual","EstoqueMin","LeadTimeDias"]
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df["Ativo"] = df["Ativo"].astype(str).str.strip().str.lower()
df["Ativo"] = df["Ativo"].map({"sim":"sim","true":"sim","1":"sim"}).fillna(df["Ativo"])

# derivadas
# margem e markup se faltarem
df["MargemPct"] = df["MargemPct"].where(df["MargemPct"].notna(),
    ((df["PrecoVenda"] - df["CustoAtual"]) / df["PrecoVenda"] * 100).replace([pd.NA, pd.NaT], 0))
df["MarkupPct"] = df["MarkupPct"].where(df["MarkupPct"].notna(),
    ((df["PrecoVenda"] / df["CustoAtual"] - 1) * 100))

df["ValorEstoque"] = (df["CustoAtual"].fillna(0) * df["EstoqueAtual"].fillna(0))
df["AbaixoMin"] = (df["EstoqueAtual"].fillna(0) <= df["EstoqueMin"].fillna(0))

# Filtros
st.sidebar.header("Filtros")
cat_sel = st.sidebar.multiselect(
    "Categoria", sorted([x for x in df["Categoria"].dropna().astype(str).unique()]))
forn_sel = st.sidebar.multiselect(
    "Fornecedor", sorted([x for x in df["Fornecedor"].dropna().astype(str).unique()]))
apenas_ativos = st.sidebar.checkbox("Somente ativos", value=True)
busca = st.sidebar.text_input("Buscar por nome/ID")

mask = pd.Series(True, index=df.index)
if cat_sel: mask &= df["Categoria"].astype(str).isin(cat_sel)
if forn_sel: mask &= df["Fornecedor"].astype(str).isin(forn_sel)
if apenas_ativos: mask &= (df["Ativo"].fillna("") == "sim")
if busca:
    s = busca.lower()
    mask &= df.apply(lambda r: s in " ".join([str(x).lower() for x in r.values]), axis=1)

dfv = df[mask].copy()

# KPIs
total_skus = len(dfv)
ativos = (dfv["Ativo"] == "sim").sum()
valor_estoque = dfv["ValorEstoque"].sum()
abaixo_min = dfv["AbaixoMin"].sum()

k1,k2,k3,k4 = st.columns(4)
k1.metric("SKUs exibidos", f"{total_skus}")
k2.metric("Ativos (sim)", f"{ativos}")
k3.metric("💰 Valor em estoque", f"R$ {valor_estoque:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
k4.metric("⚠️ Itens abaixo do mínimo", f"{abaixo_min}")

st.divider()

# Alertas: ruptura e sugestão de compra
st.subheader("⚠️ Alerta de ruptura / Sugestão de compra")
df_alerta = dfv[dfv["AbaixoMin"]].copy()
df_alerta["SugestaoCompra"] = (df_alerta["EstoqueMin"].fillna(0)*2 - df_alerta["EstoqueAtual"].fillna(0)).clip(lower=0).round()
cols_alerta = ["ID","Nome","Categoria","Fornecedor","EstoqueAtual","EstoqueMin","SugestaoCompra","LeadTimeDias"]
st.dataframe(df_alerta[ [c for c in cols_alerta if c in df_alerta.columns] ],
             use_container_width=True, hide_index=True)

st.divider()

# Top valor em estoque
st.subheader("🏆 Top 10 — Valor em estoque (custo x quantidade)")
df_top = dfv.sort_values("ValorEstoque", ascending=False).head(10)
c1,c2 = st.columns([1.2,1])

with c1:
    if not df_top.empty:
        fig = px.bar(df_top, x="Nome", y="ValorEstoque", hover_data=["EstoqueAtual","CustoAtual","Categoria"])
        fig.update_layout(xaxis_title="", yaxis_title="R$ em estoque")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para o gráfico.")

with c2:
    st.caption("Tabela Top 10 (detalhes)")
    st.dataframe(df_top[["ID","Nome","Categoria","EstoqueAtual","CustoAtual","ValorEstoque"]],
                 use_container_width=True, hide_index=True, height=420)

st.divider()

# Distribuição por categoria (Valor em estoque)
st.subheader("📦 Valor em estoque por categoria")
df_cat = dfv.groupby("Categoria", dropna=False)["ValorEstoque"].sum().reset_index().sort_values("ValorEstoque", ascending=False)
if not df_cat.empty:
    c3,c4 = st.columns(2)
    with c3:
        fig2 = px.bar(df_cat, x="Categoria", y="ValorEstoque")
        fig2.update_layout(xaxis_title="", yaxis_title="R$ em estoque")
        st.plotly_chart(fig2, use_container_width=True)
    with c4:
        fig3 = px.pie(df_cat, names="Categoria", values="ValorEstoque")
        st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Sem categorias para sumarizar.")

st.divider()

# Lista completa (filtrada)
st.subheader("📋 Lista de produtos (filtrada)")
mostrar_cols = ["ID","Nome","Categoria","Fornecedor","CustoAtual","PrecoVenda","MargemPct","EstoqueAtual","EstoqueMin","ValorEstoque","Ativo"]
mostrar_cols = [c for c in mostrar_cols if c in dfv.columns]
st.dataframe(dfv[mostrar_cols], use_container_width=True, hide_index=True)
