# -*- coding: utf-8 -*-
# pages/07_Fiado_Dashboard.py
# Dashboard de Fiado — versão simples para cliente leiga (sem cadastros)

import json, unicodedata, re
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
import numpy as np
import streamlit as st
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import plotly.express as px

# -----------------------------------------------------------------------------
# Config da página
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Fiado — Visão simples", page_icon="📊", layout="wide")
st.title("📊 Fiado — Acompanhamento (visão simples)")

# Paleta (cores diferentes entre gráficos)
PALETTE = ["#636EFA","#EF553B","#00CC96","#AB63FA","#FFA15A",
           "#19D3F3","#FF6692","#B6E880","#FF97FF","#FECB52"]

# -----------------------------------------------------------------------------
# Constantes/Abas
# -----------------------------------------------------------------------------
ABA_CLIENTES = "Clientes"
ABA_FIADO    = "Fiado"
ABA_PAGT     = "Fiado_Pagamentos"

COLS_CLIENTES = ["Cliente","Telefone","Obs"]
COLS_FIADO    = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_PAGT     = ["PagamentoID","DataPagamento","Cliente","Forma","TotalPago","IDsFiado","Obs"]

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente no Secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(str(svc["private_key"]))
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente no Secrets."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

def _fmt_brl(v) -> str:
    try:
        return ("R$ "+f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except:
        return "R$ 0,00"

def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ","").replace(",", ".")
    s = re.sub(r"[^0-9.\-]","", s)
    if s.count(".")>1:
        parts = s.split("."); s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

def _to_date(s: str) -> Optional[date]:
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except Exception:
            pass
    return None

@st.cache_data(ttl=30, show_spinner=False)
def load_df(aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        st.error(f"🛑 Aba '{aba}' não existe na planilha."); st.stop()
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    base_cols = {ABA_CLIENTES: COLS_CLIENTES, ABA_FIADO: COLS_FIADO, ABA_PAGT: COLS_PAGT}[aba]
    for c in base_cols:
        if c not in df.columns: df[c] = ""
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    return df.fillna("")

# -----------------------------------------------------------------------------
# Carrega dados
# -----------------------------------------------------------------------------
df_fiado = load_df(ABA_FIADO)
df_pagt  = load_df(ABA_PAGT)

df_fiado["ValorNum"]     = df_fiado["Valor"].apply(_to_float)
df_fiado["ValorPagoNum"] = df_fiado["ValorPago"].apply(_to_float)
df_fiado["Data_d"]       = df_fiado["Data"].apply(_to_date)
df_fiado["Venc_d"]       = df_fiado["Vencimento"].apply(_to_date)
df_fiado["Status_norm"]  = df_fiado["Status"].astype(str).str.strip().str.lower()

df_pagt["TotalPagoNum"]  = df_pagt["TotalPago"].apply(_to_float)
df_pagt["DataPag_d"]     = df_pagt["DataPagamento"].apply(_to_date)

# -----------------------------------------------------------------------------
# Filtros (simples)
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("🔎 Filtros")
    hoje = date.today()
    # período padrão curto (90 dias) — mais amigável
    ini_default = hoje - timedelta(days=90)
    dt_ini, dt_fim = st.date_input(
        "Período (pela Data do fiado)",
        value=(ini_default, hoje),
        format="DD/MM/YYYY"
    )
    ver_somente_abertos = st.checkbox("Mostrar apenas 'Em aberto'", value=True)

# aplica filtros básicos
mask = pd.Series([True]*len(df_fiado))
if isinstance(dt_ini, date) and isinstance(dt_fim, date):
    mask &= df_fiado["Data_d"].apply(lambda d: (d is not None) and (dt_ini <= d <= dt_fim))
if ver_somente_abertos:
    mask &= df_fiado["Status_norm"].eq("em aberto")

base = df_fiado[mask].copy()

# -----------------------------------------------------------------------------
# KPIs grandes e claros
# -----------------------------------------------------------------------------
base["AtrasoDias"] = base["Venc_d"].apply(lambda d: (hoje - d).days if (d and hoje>d) else 0)
total_em_aberto = float(base["ValorNum"].sum())
qtd_clientes = base["Cliente"].nunique()
qtd_vencidos = int((base["AtrasoDias"] > 0).sum())

# pagamentos no mesmo período (para ter noção do que entrou)
mask_p = pd.Series([True]*len(df_pagt))
if isinstance(dt_ini, date) and isinstance(dt_fim, date):
    mask_p &= df_pagt["DataPag_d"].apply(lambda d: (d is not None) and (dt_ini <= d <= dt_fim))
total_pago_periodo = float(df_pagt[mask_p]["TotalPagoNum"].sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 Total em aberto", _fmt_brl(total_em_aberto))
k2.metric("🟢 Pagos no período", _fmt_brl(total_pago_periodo))
k3.metric("👥 Clientes com fiado", f"{qtd_clientes}")
k4.metric("⚠️ Lançamentos vencidos", f"{qtd_vencidos}")

st.markdown("---")

# -----------------------------------------------------------------------------
# Gráfico A: Em aberto por faixa de atraso (simples e colorido)
# -----------------------------------------------------------------------------
if not base.empty:
    def bucket(dias: int) -> str:
        if dias <= 0: return "No prazo"
        if dias <= 7: return "1–7 dias"
        if dias <= 30: return "8–30 dias"
        if dias <= 60: return "31–60 dias"
        if dias <= 90: return "61–90 dias"
        return ">90 dias"

    dados_aging = base.copy()
    dados_aging["Faixa de atraso"] = dados_aging["AtrasoDias"].apply(bucket)
    aging = (dados_aging.groupby("Faixa de atraso", as_index=False)["ValorNum"]
                        .sum()
                        .rename(columns={"ValorNum":"Valor"}))

    ordem = ["No prazo","1–7 dias","8–30 dias","31–60 dias","61–90 dias",">90 dias"]
    aging["Faixa de atraso"] = pd.Categorical(aging["Faixa de atraso"], categories=ordem, ordered=True)
    aging = aging.sort_values("Faixa de atraso")

    figA = px.bar(
        aging, x="Faixa de atraso", y="Valor",
        text=aging["Valor"].apply(_fmt_brl),
        title="Em aberto por faixa de atraso",
        color="Faixa de atraso",
        color_discrete_sequence=PALETTE
    )
    figA.update_traces(textposition="outside")
    figA.update_yaxes(title=None, showgrid=True)
    figA.update_xaxes(title=None)
    st.plotly_chart(figA, use_container_width=True)
else:
    st.info("Sem fiados para exibir o gráfico de atraso.")

# -----------------------------------------------------------------------------
# Gráfico B: Top devedores (em aberto)
# -----------------------------------------------------------------------------
em_aberto = base.copy()
if not em_aberto.empty:
    topN = st.slider("Top devedores (quantidade)", 3, 15, 7, step=1)
    top_dev = (em_aberto.groupby("Cliente", as_index=False)["ValorNum"]
                        .sum()
                        .rename(columns={"ValorNum":"Total em aberto"})
                        .sort_values("Total em aberto", ascending=False)
                        .head(topN))
    figB = px.bar(
        top_dev, x="Cliente", y="Total em aberto",
        title=f"Top {len(top_dev)} devedores",
        text=top_dev["Total em aberto"].apply(_fmt_brl),
        color="Cliente",
        color_discrete_sequence=PALETTE
    )
    figB.update_traces(textposition="outside")
    figB.update_yaxes(title=None, showgrid=True)
    figB.update_xaxes(title=None)
    st.plotly_chart(figB, use_container_width=True)
else:
    st.info("Sem valores em aberto para exibir Top devedores.")

st.markdown("---")

# -----------------------------------------------------------------------------
# Gráfico C: Como os pagamentos foram feitos (no período) — barras + pizza
# -----------------------------------------------------------------------------
pagt_periodo = df_pagt[mask_p].copy()
if not pagt_periodo.empty:
    dist_forma = (pagt_periodo.groupby("Forma", as_index=False)["TotalPagoNum"]
                              .sum()
                              .rename(columns={"TotalPagoNum":"Pago"})
                              .sort_values("Pago", ascending=False))
    colA, colB = st.columns([1.2, 1])
    with colA:
        figC1 = px.bar(
            dist_forma, x="Forma", y="Pago",
            title="Pagamentos por forma",
            text=dist_forma["Pago"].apply(_fmt_brl),
            color="Forma",
            color_discrete_sequence=PALETTE
        )
        figC1.update_traces(textposition="outside")
        figC1.update_yaxes(title=None, showgrid=True)
        figC1.update_xaxes(title=None)
        st.plotly_chart(figC1, use_container_width=True)
    with colB:
        figC2 = px.pie(
            dist_forma, names="Forma", values="Pago",
            title="Participação por forma",
            color="Forma",
            color_discrete_sequence=PALETTE
        )
        st.plotly_chart(figC2, use_container_width=True)
else:
    st.info("Sem pagamentos no período.")

# -----------------------------------------------------------------------------
# Listas simples: Próximos vencimentos e Vencidos
# -----------------------------------------------------------------------------
c1, c2 = st.columns(2)

with c1:
    st.subheader("📅 Próximos vencimentos")
    prox = em_aberto.copy()
    if not prox.empty:
        prox = prox[prox["Venc_d"].notna() & (prox["AtrasoDias"] <= 0)]
        prox = prox.sort_values("Venc_d").head(50)
        cols = ["Data","Cliente","Valor","Vencimento","Obs"]
        cols = [c for c in cols if c in prox.columns]
        if prox.empty:
            st.write("Nenhum próximo vencimento.")
        else:
            prox = prox.assign(Valor=prox["ValorNum"].apply(_fmt_brl))
            st.dataframe(prox[cols], use_container_width=True, hide_index=True)
    else:
        st.write("—")

with c2:
    st.subheader("⚠️ Vencidos")
    venc = em_aberto.copy()
    if not venc.empty:
        venc = venc[venc["AtrasoDias"] > 0].sort_values(["AtrasoDias","ValorNum"], ascending=[False,False]).head(100)
        cols = ["Data","Cliente","Valor","Vencimento","Obs"]
        cols = [c for c in cols if c in venc.columns]
        if venc.empty:
            st.write("Nenhum vencido.")
        else:
            venc = venc.assign(Valor=venc["ValorNum"].apply(_fmt_brl))
            st.dataframe(venc[cols], use_container_width=True, hide_index=True)
    else:
        st.write("—")

st.markdown("---")

# -----------------------------------------------------------------------------
# Exportar visão (CSV)
# -----------------------------------------------------------------------------
st.subheader("⬇️ Exportar visão atual")
cols_export = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs"]
cols_export = [c for c in cols_export if c in base.columns]
exp = base.copy()
exp["Valor"] = exp["ValorNum"].apply(_fmt_brl)
csv_all = exp[cols_export].to_csv(index=False).encode("utf-8-sig")
st.download_button("Exportar (CSV)", data=csv_all, file_name="fiado_visao_simples.csv")
