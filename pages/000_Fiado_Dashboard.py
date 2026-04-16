# -*- coding: utf-8 -*-
# pages/000_Fiado_Dashboard.py — versão redesenhada para usuária leiga

import json, unicodedata, re
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import plotly.express as px

# =========================
# Config
# =========================
st.set_page_config(page_title="Fiado", page_icon="💳", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

/* Header */
.fiado-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px;
    padding: 28px 36px;
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.3);
}
.fiado-header-title {
    font-family: 'Nunito', sans-serif;
    font-weight: 900;
    font-size: 1.9rem;
    color: #fff;
    margin: 0;
}
.fiado-header-sub {
    font-size: 0.85rem;
    color: rgba(255,255,255,0.5);
    margin-top: 4px;
}
.fiado-badge-warn {
    background: rgba(248,113,113,0.2);
    border: 1px solid rgba(248,113,113,0.4);
    border-radius: 50px;
    padding: 8px 18px;
    color: #f87171;
    font-size: 0.85rem;
    font-weight: 700;
}
.fiado-badge-ok {
    background: rgba(74,222,128,0.15);
    border: 1px solid rgba(74,222,128,0.3);
    border-radius: 50px;
    padding: 8px 18px;
    color: #4ade80;
    font-size: 0.85rem;
    font-weight: 700;
}

/* KPI Cards */
.kpi-card {
    background: rgba(255,255,255,0.06);
    border-radius: 18px;
    padding: 22px 24px;
    border: 1px solid rgba(255,255,255,0.1);
    height: 100%;
    transition: transform 0.15s ease;
}
.kpi-card:hover { transform: translateY(-2px); }
.kpi-icon { font-size: 1.6rem; margin-bottom: 8px; display: block; }
.kpi-label {
    font-size: 0.72rem;
    font-weight: 700;
    color: rgba(255,255,255,0.4);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 6px;
}
.kpi-value {
    font-family: 'Nunito', sans-serif;
    font-size: 1.7rem;
    font-weight: 900;
    color: #fff;
    line-height: 1.1;
}
.kpi-sub { font-size: 0.78rem; margin-top: 5px; }
.kpi-red   { color: #f87171; }
.kpi-green { color: #4ade80; }
.kpi-blue  { color: #60a5fa; }
.kpi-amber { color: #fbbf24; }

/* Devedor card */
.devedor-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px;
    padding: 14px 18px;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.devedor-card.vencido {
    background: rgba(248,113,113,0.08);
    border-color: rgba(248,113,113,0.25);
}
.devedor-nome {
    font-weight: 700;
    font-size: 0.95rem;
    color: rgba(255,255,255,0.9);
}
.devedor-info {
    font-size: 0.76rem;
    color: rgba(255,255,255,0.4);
    margin-top: 2px;
}
.devedor-valor {
    font-family: 'Nunito', sans-serif;
    font-weight: 800;
    font-size: 1.05rem;
    color: #f87171;
}
.devedor-valor.ok { color: #fbbf24; }

/* Tags de prazo */
.tag {
    display: inline-block;
    border-radius: 6px;
    padding: 2px 9px;
    font-size: 0.72rem;
    font-weight: 700;
    margin-left: 8px;
}
.tag-vencido { background: rgba(248,113,113,0.2); color: #f87171; }
.tag-hoje    { background: rgba(251,191,36,0.2); color: #fbbf24; }
.tag-ok      { background: rgba(74,222,128,0.15); color: #4ade80; }

/* Seção título */
.secao {
    font-family: 'Nunito', sans-serif;
    font-weight: 800;
    font-size: 1.05rem;
    color: rgba(255,255,255,0.85);
    margin: 28px 0 14px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.secao::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(to right, rgba(255,255,255,0.12), transparent);
    margin-left: 8px;
}

footer { display: none !important; }
#MainMenu { display: none !important; }
</style>
""", unsafe_allow_html=True)

# =========================
# Constantes
# =========================
ABA_FIADO = "Fiado"
ABA_PAGT  = "Fiado_Pagamentos"
COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_PAGT  = ["PagamentoID","DataPagamento","Cliente","Forma","TotalPago","IDsFiado","Obs"]
PALETTE    = ["#636EFA","#EF553B","#00CC96","#AB63FA","#FFA15A","#19D3F3","#FF6692","#B6E880"]

# =========================
# Helpers
from utils.sheets import (
    sheet as _sheet_obj, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque, tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
_to_num = to_num; _to_float = to_num; _brl = brl; _fmt_brl = brl
_first_col = first_col; _fmt_num = fmt_num; _parse_date_any = parse_date
_tg_send = tg_send; _tg_media = tg_media; _norm_tipo_mov = norm_tipo_mov
_gerar_id = gerar_id; _parse_date = parse_date; _norm_tipo = norm_tipo_mov
def _canon_id(x):
    import re as _re; return _re.sub(r"[^0-9]", "", str(x or ""))
def conectar_sheets(): return _sheet_obj()


@st.cache_data(ttl=30, show_spinner=False)
def load_df(aba):
    sh = conectar_sheets()
    try: ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        st.error(f"🛑 Aba '{aba}' não encontrada."); st.stop()
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    base = {ABA_FIADO: COLS_FIADO, ABA_PAGT: COLS_PAGT}[aba]
    for c in base:
        if c not in df.columns: df[c] = ""
    return df.fillna("").loc[:, ~pd.Index(df.columns).duplicated(keep="first")]

# =========================
# Dados
# =========================
df_fiado = load_df(ABA_FIADO)
df_pagt  = load_df(ABA_PAGT)

df_fiado["ValorNum"]    = df_fiado["Valor"].apply(_to_float)
df_fiado["ValorPagoNum"]= df_fiado["ValorPago"].apply(_to_float)
df_fiado["Data_d"]      = df_fiado["Data"].apply(_to_date)
df_fiado["Venc_d"]      = df_fiado["Vencimento"].apply(_to_date)
df_fiado["Status_norm"] = df_fiado["Status"].astype(str).str.strip().str.lower()
df_pagt["TotalPagoNum"] = df_pagt["TotalPago"].apply(_to_float)
df_pagt["DataPag_d"]    = df_pagt["DataPagamento"].apply(_to_date)

hoje = date.today()

# =========================
# Filtro de período — sidebar simples
# =========================
with st.sidebar:
    st.markdown("### 📅 Período")
    ini_default = hoje - timedelta(days=90)
    periodo = st.date_input("", value=(ini_default, hoje), format="DD/MM/YYYY", label_visibility="collapsed")
    dt_ini = periodo[0] if isinstance(periodo, tuple) and len(periodo) > 0 else ini_default
    dt_fim = periodo[1] if isinstance(periodo, tuple) and len(periodo) > 1 else hoje

    st.markdown("---")
    ver_todos = st.checkbox("Ver também os já pagos", value=False)

# =========================
# Filtragem
# =========================
mask = df_fiado["Data_d"].apply(lambda d: d is not None and dt_ini <= d <= dt_fim)
if not ver_todos:
    mask &= df_fiado["Status_norm"].eq("em aberto")
base = df_fiado[mask].copy()
base["AtrasoDias"] = base["Venc_d"].apply(lambda d: max(0, (hoje - d).days) if d and hoje > d else 0)

mask_p = df_pagt["DataPag_d"].apply(lambda d: d is not None and dt_ini <= d <= dt_fim)
pagt_periodo = df_pagt[mask_p].copy()

# =========================
# Totais
# =========================
total_aberto    = float(base["ValorNum"].sum())
total_pago      = float(pagt_periodo["TotalPagoNum"].sum())
qtd_clientes    = base["Cliente"].nunique()
qtd_vencidos    = int((base["AtrasoDias"] > 0).sum())
valor_vencido   = float(base[base["AtrasoDias"] > 0]["ValorNum"].sum())

# =========================
# HEADER
# =========================
badge = (f'<span class="fiado-badge-warn">⚠️ {qtd_vencidos} vencido{"s" if qtd_vencidos!=1 else ""}</span>'
         if qtd_vencidos > 0
         else '<span class="fiado-badge-ok">✅ Tudo no prazo</span>')

st.markdown(f"""
<div class="fiado-header">
    <div>
        <div class="fiado-header-title">💳 Fiado</div>
        <div class="fiado-header-sub">Acompanhamento de {dt_ini.strftime('%d/%m/%Y')} a {dt_fim.strftime('%d/%m/%Y')}</div>
    </div>
    {badge}
</div>
""", unsafe_allow_html=True)

# =========================
# KPI CARDS
# =========================
c1, c2, c3, c4 = st.columns(4)

def kpi(icon, label, value, sub="", sub_class=""):
    return f"""<div class="kpi-card">
        <span class="kpi-icon">{icon}</span>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {'<div class="kpi-sub ' + sub_class + '">' + sub + '</div>' if sub else ''}
    </div>"""

with c1:
    st.markdown(kpi("💰", "Total em aberto", _fmt_brl(total_aberto),
                f"{qtd_clientes} cliente{'s' if qtd_clientes!=1 else ''}", "kpi-blue"), unsafe_allow_html=True)
with c2:
    st.markdown(kpi("✅", "Recebido no período", _fmt_brl(total_pago), "Pagamentos confirmados", "kpi-green"), unsafe_allow_html=True)
with c3:
    cor = "kpi-red" if qtd_vencidos > 0 else "kpi-green"
    sub = _fmt_brl(valor_vencido) + " em atraso" if qtd_vencidos > 0 else "Tudo no prazo"
    st.markdown(kpi("⚠️", "Vencidos", str(qtd_vencidos), sub, cor), unsafe_allow_html=True)
with c4:
    # próximo vencimento
    proximos = base[base["AtrasoDias"] == 0].sort_values("Venc_d")
    if not proximos.empty and proximos["Venc_d"].notna().any():
        prox_venc = proximos["Venc_d"].dropna().iloc[0]
        dias_prox = (prox_venc - hoje).days
        prox_txt  = prox_venc.strftime("%d/%m")
        prox_sub  = f"daqui {dias_prox} dia{'s' if dias_prox!=1 else ''}" if dias_prox > 0 else "hoje"
        st.markdown(kpi("📅", "Próximo vencimento", prox_txt, prox_sub, "kpi-amber"), unsafe_allow_html=True)
    else:
        st.markdown(kpi("📅", "Próximo vencimento", "—", "Sem datas futuras", ""), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# =========================
# QUEM ESTÁ DEVENDO (cards por cliente)
# =========================
st.markdown('<div class="secao">👥 Quem está com fiado em aberto</div>', unsafe_allow_html=True)

if not base.empty:
    por_cliente = (base.groupby("Cliente", as_index=False)
                   .agg(Total=("ValorNum","sum"), MaxAtraso=("AtrasoDias","max"), Qtd=("ID","count"))
                   .sort_values("Total", ascending=False))

    for _, row in por_cliente.iterrows():
        nome     = str(row["Cliente"]).strip().title()
        total    = row["Total"]
        atraso   = int(row["MaxAtraso"])
        qtd      = int(row["Qtd"])
        vencido  = atraso > 0

        if vencido:
            tag = f'<span class="tag tag-vencido">⚠️ {atraso}d atraso</span>'
            card_class = "devedor-card vencido"
        else:
            tag = '<span class="tag tag-ok">✅ No prazo</span>'
            card_class = "devedor-card"

        st.markdown(f"""
        <div class="{card_class}">
            <div>
                <div class="devedor-nome">{nome}{tag}</div>
                <div class="devedor-info">{qtd} lançamento{"s" if qtd!=1 else ""}</div>
            </div>
            <div class="devedor-valor {'ok' if not vencido else ''}">{_fmt_brl(total)}</div>
        </div>
        """, unsafe_allow_html=True)
else:
    st.info("Nenhum fiado em aberto no período.")

st.markdown("<br>", unsafe_allow_html=True)

# =========================
# GRÁFICO: Por faixa de atraso
# =========================
if not base.empty:
    st.markdown('<div class="secao">📊 Situação dos valores em aberto</div>', unsafe_allow_html=True)

    def bucket(dias):
        if dias <= 0: return "No prazo"
        if dias <= 7: return "1–7 dias"
        if dias <= 30: return "8–30 dias"
        if dias <= 60: return "31–60 dias"
        return "Mais de 60 dias"

    base["Faixa"] = base["AtrasoDias"].apply(bucket)
    aging = base.groupby("Faixa", as_index=False)["ValorNum"].sum().rename(columns={"ValorNum":"Valor"})
    ordem = ["No prazo","1–7 dias","8–30 dias","31–60 dias","Mais de 60 dias"]
    aging["Faixa"] = pd.Categorical(aging["Faixa"], categories=ordem, ordered=True)
    aging = aging.sort_values("Faixa")

    cor_map = {
        "No prazo":        "#4ade80",
        "1–7 dias":        "#fbbf24",
        "8–30 dias":       "#fb923c",
        "31–60 dias":      "#f87171",
        "Mais de 60 dias": "#dc2626",
    }

    figA = px.bar(
        aging, x="Faixa", y="Valor",
        text=aging["Valor"].apply(_fmt_brl),
        color="Faixa",
        color_discrete_map=cor_map,
        template="plotly_dark",
    )
    figA.update_traces(textposition="outside", marker_line_width=0)
    figA.update_layout(
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="rgba(255,255,255,0.7)",
        font_family="DM Sans",
        yaxis=dict(title=None, showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
        xaxis=dict(title=None),
        margin=dict(l=0, r=0, t=10, b=0),
        height=280,
    )
    st.plotly_chart(figA, use_container_width=True)

# =========================
# GRÁFICO: Recebimentos por forma de pagamento
# =========================
if not pagt_periodo.empty:
    st.markdown('<div class="secao">💳 Como os pagamentos foram feitos</div>', unsafe_allow_html=True)

    dist = (pagt_periodo.groupby("Forma", as_index=False)["TotalPagoNum"]
            .sum().rename(columns={"TotalPagoNum":"Pago"})
            .sort_values("Pago", ascending=False))

    col1, col2 = st.columns([1.3, 1])
    with col1:
        figP = px.bar(dist, x="Forma", y="Pago",
                      text=dist["Pago"].apply(_fmt_brl),
                      color="Forma",
                      color_discrete_sequence=PALETTE,
                      template="plotly_dark")
        figP.update_traces(textposition="outside", marker_line_width=0, showlegend=False)
        figP.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.7)", font_family="DM Sans",
            yaxis=dict(title=None, showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
            xaxis=dict(title=None), showlegend=False,
            margin=dict(l=0, r=0, t=10, b=0), height=260,
        )
        st.plotly_chart(figP, use_container_width=True)
    with col2:
        figPie = px.pie(dist, names="Forma", values="Pago",
                        color="Forma", color_discrete_sequence=PALETTE,
                        hole=0.45)
        figPie.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.7)", font_family="DM Sans",
            showlegend=True, margin=dict(l=0, r=0, t=10, b=0), height=260,
            legend=dict(orientation="v", font_size=12),
        )
        figPie.update_traces(textinfo="percent", textfont_size=13)
        st.plotly_chart(figPie, use_container_width=True)

# =========================
# PRÓXIMOS VENCIMENTOS (tabela limpa)
# =========================
prox_tabela = base[(base["AtrasoDias"] == 0) & (base["Venc_d"].notna())].sort_values("Venc_d").head(20)
if not prox_tabela.empty:
    st.markdown('<div class="secao">📅 Próximos vencimentos</div>', unsafe_allow_html=True)
    df_show = prox_tabela[["Cliente","Valor","Vencimento","Obs"]].copy()
    df_show["Cliente"] = df_show["Cliente"].str.title()
    df_show["Valor"]   = prox_tabela["ValorNum"].apply(_fmt_brl)
    df_show["Dias para vencer"] = prox_tabela["Venc_d"].apply(lambda d: f"{(d-hoje).days}d" if d else "—")
    st.dataframe(df_show, use_container_width=True, hide_index=True)
