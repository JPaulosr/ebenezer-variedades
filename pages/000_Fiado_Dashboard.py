# -*- coding: utf-8 -*-
# pages/07_Fiado_Dashboard.py
# Dashboard de Fiado — visualização e acompanhamento (sem cadastros)
# Requer as abas: Clientes, Fiado, Fiado_Pagamentos (mesmas do 06_fiado.py)

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
# UI / Página
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Dashboard — Fiado", page_icon="📊", layout="wide")
st.title("📊 Dashboard — Fiado (Ebenezér Variedades)")

# -----------------------------------------------------------------------------
# Helpers gerais / mesma base do 06_fiado.py sempre que possível
# -----------------------------------------------------------------------------
ABA_CLIENTES = "Clientes"
ABA_FIADO    = "Fiado"
ABA_PAGT     = "Fiado_Pagamentos"

COLS_CLIENTES = ["Cliente","Telefone","Obs"]
COLS_FIADO    = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_PAGT     = ["PagamentoID","DataPagamento","Cliente","Forma","TotalPago","IDsFiado","Obs"]

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
    # garante colunas mínimas
    base_cols = {ABA_CLIENTES: COLS_CLIENTES, ABA_FIADO: COLS_FIADO, ABA_PAGT: COLS_PAGT}[aba]
    for c in base_cols:
        if c not in df.columns: df[c] = ""
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    return df.fillna("")

# -----------------------------------------------------------------------------
# Carregamento de dados
# -----------------------------------------------------------------------------
df_fiado = load_df(ABA_FIADO)
df_pagt  = load_df(ABA_PAGT)
df_cli   = load_df(ABA_CLIENTES)

# Tipagens
df_fiado["ValorNum"]      = df_fiado["Valor"].apply(_to_float)
df_fiado["ValorPagoNum"]  = df_fiado["ValorPago"].apply(_to_float)
df_fiado["Data_d"]        = df_fiado["Data"].apply(_to_date)
df_fiado["Venc_d"]        = df_fiado["Vencimento"].apply(_to_date)
df_fiado["Status_norm"]   = df_fiado["Status"].astype(str).str.strip().str.lower()

df_pagt["TotalPagoNum"]   = df_pagt["TotalPago"].apply(_to_float)
df_pagt["DataPag_d"]      = df_pagt["DataPagamento"].apply(_to_date)

# -----------------------------------------------------------------------------
# Filtros de Dashboard
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("🔎 Filtros")
    hoje = date.today()
    # intervalo padrão: últimos 180 dias
    ini_default = hoje - timedelta(days=180)
    dt_ini, dt_fim = st.date_input(
        "Período (pela Data do fiado)",
        value=(ini_default, hoje),
        format="DD/MM/YYYY"
    )
    status_opt = st.selectbox("Status", ["Todos", "Em aberto", "Pago"], index=0)
    clientes = sorted([c for c in df_fiado["Cliente"].astype(str).str.strip().unique().tolist() if c])
    cli_sel = st.multiselect("Cliente(s)", options=clientes, default=[])

# Aplica filtros
mask = pd.Series([True]*len(df_fiado))
if isinstance(dt_ini, date) and isinstance(dt_fim, date):
    mask &= df_fiado["Data_d"].apply(lambda d: (d is not None) and (dt_ini <= d <= dt_fim))
if status_opt != "Todos":
    mask &= df_fiado["Status_norm"].eq(status_opt.lower())
if cli_sel:
    mask &= df_fiado["Cliente"].isin(cli_sel)

base = df_fiado[mask].copy()

# -----------------------------------------------------------------------------
# KPIs principais
# -----------------------------------------------------------------------------
base["AtrasoDias"] = base["Venc_d"].apply(lambda d: (hoje - d).days if (d and hoje>d) else 0)
abertos = base[base["Status_norm"]=="em aberto"].copy()
pagos   = base[base["Status_norm"]=="pago"].copy()

total_em_aberto = float(abertos["ValorNum"].sum())
total_pago_periodo = float(pagos["ValorNum"].sum())  # somamos valor original das linhas pagas no período filtrado
qtde_clientes_aberto = abertos["Cliente"].nunique()
qtde_lancamentos = len(base)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total em aberto", _fmt_brl(total_em_aberto))
c2.metric("Total pago no período", _fmt_brl(total_pago_periodo))
c3.metric("Clientes com fiado em aberto", f"{qtde_clientes_aberto}")
c4.metric("Lançamentos no período", f"{qtde_lancamentos}")

st.markdown("---")

# -----------------------------------------------------------------------------
# Gráfico 1: Evolução mensal (emissão x pagamentos)
# -----------------------------------------------------------------------------
def _as_year_month(d: Optional[date]) -> str:
    return d.strftime("%Y-%m") if d else "0000-00"

evol_fiado = base.copy()
evol_fiado["YM"] = evol_fiado["Data_d"].apply(_as_year_month)
grp_fiado = evol_fiado.groupby("YM", as_index=False)["ValorNum"].sum().rename(columns={"ValorNum":"FiadoEmitido"})

# pagamentos do período (usa df_pagt, filtrado por período)
mask_p = pd.Series([True]*len(df_pagt))
if isinstance(dt_ini, date) and isinstance(dt_fim, date):
    mask_p &= df_pagt["DataPag_d"].apply(lambda d: (d is not None) and (dt_ini <= d <= dt_fim))
if cli_sel:
    mask_p &= df_pagt["Cliente"].isin(cli_sel)
pagt_periodo = df_pagt[mask_p].copy()
pagt_periodo["YM"] = pagt_periodo["DataPag_d"].apply(_as_year_month)
grp_pagt = pagt_periodo.groupby("YM", as_index=False)["TotalPagoNum"].sum().rename(columns={"TotalPagoNum":"Pagamentos"})

serie = pd.merge(grp_fiado, grp_pagt, on="YM", how="outer").fillna(0.0).sort_values("YM")
fig1 = px.line(serie, x="YM", y=["FiadoEmitido","Pagamentos"], markers=True,
               title="Evolução mensal — Fiado emitido vs. Pagamentos")
st.plotly_chart(fig1, use_container_width=True)

# -----------------------------------------------------------------------------
# Gráfico 2: Aging (atrasos) — barras empilhadas por faixas
# -----------------------------------------------------------------------------
if not abertos.empty:
    def bucket(dias: int) -> str:
        if dias <= 0: return "No prazo"
        if dias <= 7: return "1–7 dias"
        if dias <= 30: return "8–30 dias"
        if dias <= 60: return "31–60 dias"
        if dias <= 90: return "61–90 dias"
        return ">90 dias"

    abertos["FaixaAtraso"] = abertos["AtrasoDias"].apply(bucket)
    aging = abertos.groupby("FaixaAtraso", as_index=False)["ValorNum"].sum()
    # Ordena faixas em ordem lógica
    ordem = ["No prazo","1–7 dias","8–30 dias","31–60 dias","61–90 dias",">90 dias"]
    aging["FaixaAtraso"] = pd.Categorical(aging["FaixaAtraso"], categories=ordem, ordered=True)
    aging = aging.sort_values("FaixaAtraso")
    fig2 = px.bar(aging, x="FaixaAtraso", y="ValorNum", text="ValorNum",
                  title="Aging — Em aberto por faixa de atraso")
    fig2.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Sem fiados em aberto dentro dos filtros para exibir o Aging.")

# -----------------------------------------------------------------------------
# Gráfico 3: Top devedores (em aberto)
# -----------------------------------------------------------------------------
if not abertos.empty:
    topN = st.slider("Top devedores (N)", 3, 20, 10, step=1)
    top_dev = (abertos.groupby("Cliente", as_index=False)["ValorNum"]
                      .sum()
                      .rename(columns={"ValorNum":"TotalEmAberto"})
                      .sort_values("TotalEmAberto", ascending=False)
                      .head(topN))
    fig3 = px.bar(top_dev, x="Cliente", y="TotalEmAberto", title=f"Top {len(top_dev)} devedores (em aberto)")
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Sem valores em aberto para exibir Top devedores.")

st.markdown("---")

# -----------------------------------------------------------------------------
# Gráfico 4: Distribuição por forma de pagamento (com base nos registros da aba de pagamentos)
# -----------------------------------------------------------------------------
if not pagt_periodo.empty:
    dist_forma = (pagt_periodo.groupby("Forma", as_index=False)["TotalPagoNum"].sum()
                              .sort_values("TotalPagoNum", ascending=False))
    colA, colB = st.columns([1.2, 1])
    with colA:
        fig4 = px.bar(dist_forma, x="Forma", y="TotalPagoNum", title="Pagamentos por forma")
        st.plotly_chart(fig4, use_container_width=True)
    with colB:
        fig4p = px.pie(dist_forma, names="Forma", values="TotalPagoNum", title="Participação por forma")
        st.plotly_chart(fig4p, use_container_width=True)
else:
    st.info("Sem pagamentos no período para exibir distribuição por forma.")

# -----------------------------------------------------------------------------
# Tabelas operacionais — próximos vencimentos e vencidos
# -----------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Próximos vencimentos (em aberto)")
    if not abertos.empty:
        proximos = abertos.copy()
        proximos = proximos[proximos["Venc_d"].notna()]
        proximos = proximos[proximos["AtrasoDias"] <= 0]  # no prazo
        proximos = proximos.sort_values("Venc_d").head(50)
        if proximos.empty:
            st.write("Nenhum próximo vencimento nos filtros.")
        else:
            show_cols = ["ID","Data","Cliente","Valor","Vencimento","Obs"]
            show_cols = [c for c in show_cols if c in proximos.columns]
            st.dataframe(proximos[show_cols], use_container_width=True, hide_index=True)
    else:
        st.write("—")

with col2:
    st.subheader("⚠️ Vencidos (em aberto)")
    if not abertos.empty:
        vencidos = abertos[abertos["AtrasoDias"] > 0].copy().sort_values(["AtrasoDias","ValorNum"], ascending=[False,False]).head(100)
        if vencidos.empty:
            st.write("Nenhum vencido nos filtros.")
        else:
            show_cols = ["ID","Data","Cliente","Valor","Vencimento","AtrasoDias","Obs"]
            show_cols = [c for c in show_cols if c in vencidos.columns]
            st.dataframe(vencidos[show_cols], use_container_width=True, hide_index=True)
    else:
        st.write("—")

st.markdown("---")

# -----------------------------------------------------------------------------
# Detalhe por cliente (drill-down)
# -----------------------------------------------------------------------------
st.subheader("🔍 Detalhe por cliente")
cli_one = st.selectbox("Selecionar cliente", options=[""] + clientes, index=0)
if cli_one:
    det = df_fiado.copy()
    # aplica período do filtro lateral
    if isinstance(dt_ini, date) and isinstance(dt_fim, date):
        det = det[det["Data_d"].apply(lambda d: (d is not None) and (dt_ini <= d <= dt_fim))]
    det = det[det["Cliente"]==cli_one].copy()
    if det.empty:
        st.info("Sem lançamentos deste cliente no período.")
    else:
        det["AtrasoDias"] = det["Venc_d"].apply(lambda d: (hoje - d).days if (d and hoje>d) else 0)
        k1, k2, k3 = st.columns(3)
        em_aberto_cli = float(det[det["Status_norm"]=="em aberto"]["ValorNum"].sum())
        pago_cli      = float(det[det["Status_norm"]=="pago"]["ValorNum"].sum())
        qtd_cli       = len(det)
        k1.metric("Em aberto (cliente)", _fmt_brl(em_aberto_cli))
        k2.metric("Pago no período (cliente)", _fmt_brl(pago_cli))
        k3.metric("Qtde lançamentos", f"{qtd_cli}")

        # Evolução do cliente
        det["YM"] = det["Data_d"].apply(_as_year_month)
        evol_cli = det.groupby("YM", as_index=False)["ValorNum"].sum()
        figc = px.line(evol_cli, x="YM", y="ValorNum", markers=True, title=f"Evolução — {cli_one}")
        st.plotly_chart(figc, use_container_width=True)

        # Tabela detalhada
        cols_show = ["ID","Data","Cliente","Valor","Vencimento","Status","DataPagamento","FormaPagamento","ValorPago","Obs","AtrasoDias"]
        cols_show = [c for c in cols_show if c in det.columns]
        st.dataframe(det.sort_values(["Status_norm","Data_d"], ascending=[True, False])[cols_show],
                     use_container_width=True, hide_index=True)

        # Export do cliente
        csv_cli = det[cols_show].to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Exportar cliente (CSV)", data=csv_cli, file_name=f"fiado_{cli_one.replace(' ','_')}.csv")

st.markdown("---")

# -----------------------------------------------------------------------------
# Export geral dos dados filtrados
# -----------------------------------------------------------------------------
st.subheader("⬇️ Exportar visão filtrada")
cols_export = ["ID","Data","Cliente","Valor","Vencimento","Status","DataPagamento","FormaPagamento","ValorPago","Obs"]
cols_export = [c for c in cols_export if c in base.columns]
csv_all = base[cols_export].to_csv(index=False).encode("utf-8-sig")
st.download_button("Exportar (CSV)", data=csv_all, file_name="fiado_dashboard_filtrado.csv")
