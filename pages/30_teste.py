# pages/30_dashboard.py — Dashboard Ebenezér Variedades
# -*- coding: utf-8 -*-
import unicodedata, json
from datetime import datetime, timedelta
from typing import Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

# =========================
# CONFIG VISUAL
# =========================
st.set_page_config(page_title="Ebenezér Variedades — Dashboard", page_icon="🧮", layout="wide")
st.title("🧮 Dashboard — Ebenezér Variedades")

# =========================
# HELPERS: Secrets & Auth
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): 
        return key
    key = key.replace("\\n", "\n")
    # remove caracteres de controle exceto \n, \r, \t
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_service_account_from_secrets() -> Optional[dict]:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("❌ Configure o segredo `GCP_SERVICE_ACCOUNT` nos Secrets do app.")
        return None
    if isinstance(svc, str):
        try:
            svc = json.loads(svc)
        except Exception:
            st.error("❌ `GCP_SERVICE_ACCOUNT` nos Secrets não está em JSON válido.")
            return None
    if "private_key" in svc:
        svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource(show_spinner=False)
def _open_sheet(sheet_id: str):
    svc = _load_service_account_from_secrets()
    if not svc: 
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def _read_ws_as_df(sh, name: str) -> pd.DataFrame:
    try:
        ws = sh.worksheet(name)
    except Exception:
        return pd.DataFrame()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    # limpa colunas vazias e linhas totalmente NaN
    df = df.dropna(how="all")
    # padroniza nomes de colunas
    df.columns = [str(c).strip() for c in df.columns]
    return df

# =========================
# PARÂMETROS (mude aqui se quiser)
# =========================
# Dica: você pode colocar SHEET_ID nos secrets como SHEET_ID_EBENEZER
SHEET_ID = st.secrets.get("SHEET_ID_EBENEZER") or st.secrets.get("SHEET_ID") or ""
NOME_ABA_VENDAS   = st.secrets.get("WS_VENDAS")   or "vendas"
NOME_ABA_COMPRAS  = st.secrets.get("WS_COMPRAS")  or "compras"
NOME_ABA_PRODUTOS = st.secrets.get("WS_PRODUTOS") or "produtos"
NOME_ABA_FIADO    = st.secrets.get("WS_FIADO")    or "fiado"   # opcional

if not SHEET_ID:
    st.warning("⚠️ Defina `SHEET_ID_EBENEZER` (ou `SHEET_ID`) nos Secrets.")
else:
    sh = _open_sheet(SHEET_ID)

# =========================
# Normalização de colunas
# =========================
def _first_existing(d: dict, keys: list, default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default

def _norm_cols(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Renomeia colunas se alguma das alternativas existir."""
    rename = {}
    lc = {c.lower(): c for c in df.columns}
    for canon, alts in mapping.items():
        for a in alts:
            if a.lower() in lc:
                rename[lc[a.lower()]] = canon
                break
    if rename:
        df = df.rename(columns=rename)
    return df

def _to_num(x, default=0.0):
    if pd.isna(x):
        return default
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default

def _to_int(x, default=0):
    try:
        return int(float(str(x).strip().replace(",", ".")))
    except Exception:
        return default

def _to_date(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    s = str(x).strip()
    # tenta dd/mm/aaaa
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt, dayfirst=("d" in fmt))
        except Exception:
            pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="coerce")
    except Exception:
        return pd.NaT

# =========================
# CARREGAMENTO
# =========================
@st.cache_data(show_spinner=True, ttl=60)
def _load_all(_sheet_id: str):
    if not _sheet_id or not sh:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    df_v = _read_ws_as_df(sh, NOME_ABA_VENDAS)
    df_c = _read_ws_as_df(sh, NOME_ABA_COMPRAS)
    df_p = _read_ws_as_df(sh, NOME_ABA_PRODUTOS)
    df_f = _read_ws_as_df(sh, NOME_ABA_FIADO)

    # Normaliza colunas principais
    if not df_v.empty:
        df_v = _norm_cols(df_v, {
            "data": ["data", "dt", "dia"],
            "produto": ["produto", "item", "nome", "descrição", "descricao"],
            "sku": ["sku", "código", "codigo", "cod", "id"],
            "quantidade": ["quantidade", "qtd", "qtde", "qte"],
            "valor_unit": ["preco", "preço", "preço unitário", "preco_unitario", "valor", "valor_unit"],
            "valor_total": ["total", "valor total", "valor_total"],
            "custo_unit": ["custo", "custo_unit", "preco_compra", "preço_compra", "custo_venda"],
            "conta": ["conta", "forma_pagamento", "pagamento"],
            "cliente": ["cliente", "nome_cliente"]
        })
        # datas & nums
        if "data" in df_v: df_v["data"] = df_v["data"].apply(_to_date)
        if "quantidade" in df_v: df_v["quantidade"] = df_v["quantidade"].apply(_to_num).fillna(1).replace(0,1)
        if "valor_unit" in df_v: df_v["valor_unit"] = df_v["valor_unit"].apply(_to_num)
        if "valor_total" in df_v: df_v["valor_total"] = df_v["valor_total"].apply(_to_num)
        if "custo_unit" in df_v:  df_v["custo_unit"]  = df_v["custo_unit"].apply(_to_num)

    if not df_c.empty:
        df_c = _norm_cols(df_c, {
            "data": ["data", "dt", "dia"],
            "produto": ["produto", "item", "nome", "descrição", "descricao"],
            "sku": ["sku", "código", "codigo", "cod", "id"],
            "quantidade": ["quantidade", "qtd", "qtde", "qte"],
            "preco_compra": ["preco_compra", "preço_compra", "custo", "custo_unit", "valor_unit"],
            "valor_total": ["valor_total", "total"]
        })
        if "data" in df_c: df_c["data"] = df_c["data"].apply(_to_date)
        if "quantidade" in df_c: df_c["quantidade"] = df_c["quantidade"].apply(_to_num)
        if "preco_compra" in df_c: df_c["preco_compra"] = df_c["preco_compra"].apply(_to_num)
        if "valor_total" in df_c: df_c["valor_total"] = df_c["valor_total"].apply(_to_num)

    if not df_p.empty:
        df_p = _norm_cols(df_p, {
            "produto": ["produto", "item", "nome", "descrição", "descricao"],
            "sku": ["sku", "código", "codigo", "cod", "id"],
            "custo_padrao": ["custo", "preco_compra", "preço_compra", "custo_padrao"],
            "preco_venda_padrao": ["preco", "preço", "preco_venda", "preço_venda"]
        })
        if "custo_padrao" in df_p: df_p["custo_padrao"] = df_p["custo_padrao"].apply(_to_num)
        if "preco_venda_padrao" in df_p: df_p["preco_venda_padrao"] = df_p["preco_venda_padrao"].apply(_to_num)

    if not df_f.empty:
        df_f = _norm_cols(df_f, {
            "data": ["data", "dt", "dia"],
            "valor": ["valor", "valor_total", "total", "parcela"],
            "status": ["status", "situação", "situacao"]
        })
        if "data" in df_f: df_f["data"] = df_f["data"].apply(_to_date)
        if "valor" in df_f: df_f["valor"] = df_f["valor"].apply(_to_num)

    return df_v, df_c, df_p, df_f

df_v, df_c, df_p, df_f = _load_all(SHEET_ID)

if df_v.empty:
    st.info("Sem dados de **vendas** ainda (ou aba não encontrada).")
    st.stop()

# =========================
# Filtros (sidebar)
# =========================
with st.sidebar:
    st.header("Filtros")
    periodo = st.selectbox(
        "Período",
        ["Últimos 7 dias", "Últimos 30 dias", "Mês atual", "Últimos 90 dias", "Ano atual", "Tudo"],
        index=1,
    )
    if periodo == "Últimos 7 dias":
        dt_ini = pd.Timestamp.today().normalize() - pd.Timedelta(days=7)
    elif periodo == "Últimos 30 dias":
        dt_ini = pd.Timestamp.today().normalize() - pd.Timedelta(days=30)
    elif periodo == "Últimos 90 dias":
        dt_ini = pd.Timestamp.today().normalize() - pd.Timedelta(days=90)
    elif periodo == "Mês atual":
        today = pd.Timestamp.today().normalize()
        dt_ini = today.replace(day=1)
    elif periodo == "Ano atual":
        today = pd.Timestamp.today().normalize()
        dt_ini = today.replace(month=1, day=1)
    else:
        dt_ini = pd.Timestamp.min

    dt_fim = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)

    categoria = st.multiselect("Categoria", sorted([c for c in df_v.columns if c.lower().startswith("categoria")]), [])
    fornecedor = st.multiselect("Fornecedor", sorted([c for c in df_v.columns if c.lower().startswith("fornecedor")]), [])
    somente_ativos = st.checkbox("Somente ativos", value=True)
    busca_nome = st.text_input("Buscar por nome/ID", "")

# Aplica filtros básicos
d = df_v.copy()
if "data" in d.columns:
    d = d[(d["data"] >= dt_ini) & (d["data"] < dt_fim)]

# Filtros categoria/fornecedor se existirem colunas
for col in d.columns:
    low = col.lower()
    if low.startswith("categoria") and categoria:
        d = d[d[col].astype(str).isin(categoria)]
    if low.startswith("fornecedor") and fornecedor:
        d = d[d[col].astype(str).isin(fornecedor)]

# Filtro ativos (se existir coluna)
if somente_ativos:
    col_ativo = None
    for c in d.columns:
        if c.lower() in ("ativo", "status"):
            col_ativo = c; break
    if col_ativo:
        d = d[d[col_ativo].astype(str).str.lower().isin(["1","true","sim","ativo","yes"])]

# Busca texto
if busca_nome.strip():
    q = busca_nome.strip().lower()
    cols_busca = [c for c in ["produto", "sku", "cliente"] if c in d.columns]
    if cols_busca:
        m = pd.Series(False, index=d.index)
        for c in cols_busca:
            m = m | d[c].astype(str).str.lower().str.contains(q, na=False)
        d = d[m]

# =========================
# Cálculo: Vendas Brutas & Custo dos Itens Vendidos (COGS)
# =========================
# quantidade
if "quantidade" not in d.columns:
    d["quantidade"] = 1.0

# valor venda total
if "valor_total" in d.columns and d["valor_total"].notna().any():
    d["__total_venda"] = d["valor_total"].apply(_to_num)
else:
    # usa valor_unit * quantidade
    if "valor_unit" not in d.columns:
        d["valor_unit"] = 0.0
    d["__total_venda"] = d["valor_unit"].apply(_to_num) * d["quantidade"].apply(_to_num)

# custo unit meta:
# 1) custo_unit na venda (se existir)
# 2) custo_padrao do produto (PRODUTOS)
# 3) último preco_compra da COMPRAS por SKU/Produto
d["__custo_unit_final"] = pd.NA

# 1) custo já na venda
if "custo_unit" in d.columns:
    d["__custo_unit_final"] = d["custo_unit"].where(d["custo_unit"].notna(), pd.NA)

# 2) junta com PRODUTOS
if not df_p.empty:
    key_merge = "sku" if ("sku" in d.columns and "sku" in df_p.columns) else "produto"
    if key_merge in d.columns and key_merge in df_p.columns:
        d = d.merge(
            df_p[[key_merge, "custo_padrao"]].drop_duplicates(),
            on=key_merge, how="left"
        )
        d["__custo_unit_final"] = d["__custo_unit_final"].fillna(d["custo_padrao"])
        if "custo_padrao" in d.columns:
            d = d.drop(columns=["custo_padrao"])

# 3) pega último preço de compra por SKU/Produto
if not df_c.empty:
    key_merge = "sku" if ("sku" in d.columns and "sku" in df_c.columns) else "produto"
    if key_merge in d.columns and key_merge in df_c.columns:
        # último preço conhecido
        df_c_sorted = df_c.sort_values(by=["data"])
        ult_compra = df_c_sorted.dropna(subset=["preco_compra"]).groupby(key_merge)["preco_compra"].last().reset_index()
        ult_compra = ult_compra.rename(columns={"preco_compra":"__custo_ultimo"})
        d = d.merge(ult_compra, on=key_merge, how="left")
        d["__custo_unit_final"] = d["__custo_unit_final"].fillna(d["__custo_ultimo"])
        if "__custo_ultimo" in d.columns:
            d = d.drop(columns=["__custo_ultimo"])

# se ainda ficar NaN, vira 0 para não quebrar
d["__custo_unit_final"] = d["__custo_unit_final"].apply(lambda x: _to_num(x, 0.0))

# custo total dos itens vendidos
d["__total_custo_vendido"] = d["__custo_unit_final"] * d["quantidade"].apply(_to_num)

# métricas
vendas_brutas = float(d["__total_venda"].sum()) if not d.empty else 0.0
custo_vendido = float(d["__total_custo_vendido"].sum()) if not d.empty else 0.0
lucro_bruto   = max(vendas_brutas - custo_vendido, 0.0)
itens_vendidos = int(d["quantidade"].sum()) if not d.empty else 0

# Caixa do período = vendas à vista (se houver coluna Conta, exclui fiado)
if "conta" in d.columns:
    d_avista = d[d["conta"].astype(str).str.lower().str.contains("fiado")==False]
    caixa_periodo = float(d_avista["__total_venda"].sum())
else:
    caixa_periodo = vendas_brutas

# Fiado (opcional)
fiado_recebido = 0.0
fiado_em_aberto = 0.0
if not df_f.empty:
    # considera dentro do período
    df_f_per = df_f.copy()
    if "data" in df_f_per.columns:
        df_f_per = df_f_per[(df_f_per["data"] >= dt_ini) & (df_f_per["data"] < dt_fim)]
    col_status = "status" if "status" in df_f_per.columns else None
    if col_status:
        fiado_recebido = float(df_f_per[df_f_per[col_status].astype(str).str.lower().isin(["pago","recebido","quitado"]) ]["valor"].sum())
        fiado_em_aberto = float(df_f[df_f[col_status].astype(str).str.lower().isin(["aberto","pendente","em aberto"]) ]["valor"].sum())
    else:
        # se não há status, assume nada recebido e todo o resto em aberto
        fiado_em_aberto = float(df_f["valor"].sum())

# =========================
# CARDS
# =========================
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("🧾 Vendas no período (bruto)", f"R$ {vendas_brutas:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    st.caption(f"🗓️ Período: {dt_ini.date().strftime('%d/%m/%Y')} → {(dt_fim - pd.Timedelta(days=1)).date().strftime('%d/%m/%Y')}")

with col2:
    margem = (lucro_bruto / vendas_brutas * 100) if vendas_brutas > 0 else 0.0
    st.metric("🧮 Lucro Bruto (aprox.)", f"R$ {lucro_bruto:,.2f}".replace(",", "X").replace(".", ",").replace("X","."), f"{margem:.1f}% margem")

with col3:
    st.metric("🧳 Itens vendidos", f"{itens_vendidos:,}".replace(",", "."))

with col4:
    st.metric("💼 Caixa do período", f"R$ {caixa_periodo:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

with col5:
    st.metric("📌 Fiado em aberto (saldo)", f"R$ {fiado_em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

# =========================
# GRÁFICO — Vendas por dia
# =========================
if "data" in d.columns:
    g = d.groupby(d["data"].dt.date)["__total_venda"].sum().reset_index()
    g = g.sort_values("data")
    fig = px.bar(g, x="data", y="__total_venda", title="📅 Vendas por dia (período)", labels={"data":"Data", "__total_venda":"R$"})
    st.plotly_chart(fig, use_container_width=True)

# =========================
# TABELA DETALHADA (opcional)
# =========================
with st.expander("Ver detalhes (vendas com custos)"):
    cols_show = [c for c in ["data","sku","produto","quantidade","valor_unit","__total_venda","__custo_unit_final","__total_custo_vendido","conta","cliente"] if c in d.columns]
    if cols_show:
        dd = d[cols_show].copy()
        dd = dd.rename(columns={
            "data":"Data", "sku":"SKU", "produto":"Produto", "quantidade":"Qtd",
            "valor_unit":"Preço unitário", "__total_venda":"Total venda",
            "__custo_unit_final":"Custo unitário (usado)", "__total_custo_vendido":"Custo total"
        })
        st.dataframe(dd, use_container_width=True, hide_index=True)
    else:
        st.caption("Sem colunas para exibir.")
