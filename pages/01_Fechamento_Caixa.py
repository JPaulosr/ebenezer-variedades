# pages/01_Fechamento_Caixa.py — Fechamento de caixa (redesenhado)
# -*- coding: utf-8 -*-
from collections.abc import Mapping
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(page_title="Fechamento de Caixa", page_icon="💰",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

.page-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px; padding: 24px 32px; margin-bottom: 24px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.25);
}
.page-header h1 { font-family:'Nunito',sans-serif; font-weight:900; font-size:1.7rem; color:#fff; margin:0; }
.page-header .sub { font-size:0.82rem; color:rgba(255,255,255,0.5); margin-top:4px; }
.header-badge {
    background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
    border-radius:50px; padding:8px 18px; color:#fff; font-size:0.82rem;
    font-weight:600; backdrop-filter:blur(10px);
}

/* KPI cards */
.kpi-card {
    background:rgba(255,255,255,0.06); border-radius:18px; padding:22px 24px;
    border:1px solid rgba(255,255,255,0.1); height:100%;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.kpi-card:hover { transform:translateY(-2px); box-shadow:0 6px 24px rgba(0,0,0,0.35); }
.kpi-icon  { font-size:1.4rem; margin-bottom:8px; display:block; }
.kpi-label { font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.45);
             text-transform:uppercase; letter-spacing:0.6px; margin-bottom:6px; }
.kpi-value { font-family:'Nunito',sans-serif; font-size:1.6rem; font-weight:800; color:#fff; line-height:1.1; }
.kpi-sub   { font-size:0.75rem; color:rgba(255,255,255,0.35); margin-top:5px; }
.kpi-green { color:#4ade80; }
.kpi-blue  { color:#60a5fa; }
.kpi-yellow{ color:#fbbf24; }

/* Seção título */
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem;
    color:rgba(255,255,255,0.9); margin:28px 0 14px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.15),transparent);
    margin-left:8px; border-radius:2px;
}

/* Filtro bar */
.filtro-bar {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
    border-radius:14px; padding:16px 20px; margin-bottom:20px;
}

/* Forma pagamento pills */
.forma-pill {
    display:inline-flex; align-items:center; gap:6px;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1);
    border-radius:10px; padding:8px 14px; margin:4px;
    font-size:0.82rem; font-weight:600; color:#fff;
}
.forma-pill .val { color:#4ade80; font-family:'Nunito',sans-serif; font-size:0.95rem; font-weight:800; }

/* Tabela produto */
.prod-row {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.07);
    border-radius:12px; padding:12px 16px; margin-bottom:6px;
    display:flex; align-items:center; gap:12px;
}
.prod-nome { font-weight:700; font-size:0.88rem; color:#fff; flex:1; }
.prod-num  { font-family:'Nunito',sans-serif; font-size:0.9rem; font-weight:700; min-width:80px; text-align:right; }
.num-green { color:#4ade80; }
.num-red   { color:#f87171; }
.num-gray  { color:rgba(255,255,255,0.4); }

/* Caption info */
.info-caption {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
    border-radius:10px; padding:10px 16px; font-size:0.78rem;
    color:rgba(255,255,255,0.5); margin-top:8px;
}
</style>
""", unsafe_allow_html=True)



from utils.sheets import (
    sheet, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque,
    tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
# Aliases completos para compatibilidade com código existente
_to_num = to_num
_to_float = to_num        # mesma função, nome diferente que era usado em algumas páginas
_brl = brl
_fmt_brl = brl
_first_col = first_col
_fmt_num = fmt_num
_tg_send = tg_send
_tg_media = tg_media
_gerar_id = gerar_id
_parse_date = parse_date
_parse_date_any = parse_date
_norm_tipo_mov = norm_tipo_mov
_norm_tipo = norm_tipo_mov
conectar_sheets = sheet

def _canon_id(x):
    import re as _re
    return _re.sub(r"[^0-9]", "", str(x or ""))



# ──────────────────────────────────────────────
#  CARREGAR DADOS
# ──────────────────────────────────────────────
ABA_PROD, ABA_VEND, ABA_COMP = "Produtos", "Vendas", "Compras"
with st.spinner("Carregando dados..."):
    try: prod = carregar_aba(ABA_PROD)
    except: prod = pd.DataFrame()
    try: vend_raw = carregar_aba(ABA_VEND)
    except: vend_raw = pd.DataFrame()
    try: comp_raw = carregar_aba(ABA_COMP)
    except: comp_raw = pd.DataFrame()


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
st.markdown(f"""
<div class="page-header">
  <div>
    <h1>💰 Fechamento de Caixa</h1>
    <div class="sub">Ebenezér Variedades · {datetime.now().strftime("%d/%m/%Y %H:%M")}</div>
  </div>
  <div class="header-badge">📊 Relatório financeiro</div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  FILTROS
# ──────────────────────────────────────────────
st.markdown('<div class="filtro-bar">', unsafe_allow_html=True)
fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1])
with fc1:
    de = st.date_input("📅 De", value=date.today())
with fc2:
    ate = st.date_input("📅 Até", value=date.today())
with fc3:
    # Atalhos de período
    periodo = st.selectbox("Período rápido", ["Personalizado","Hoje","Ontem","Esta semana","Este mês","Mês passado"], index=0)
with fc4:
    inclui_estornos = st.toggle("Incluir estornos", value=False)

# Aplicar período rápido
if periodo != "Personalizado":
    hoje = date.today()
    if periodo == "Hoje":
        de = ate = hoje
    elif periodo == "Ontem":
        de = ate = hoje - timedelta(days=1)
    elif periodo == "Esta semana":
        de = hoje - timedelta(days=hoje.weekday())
        ate = hoje
    elif periodo == "Este mês":
        de = hoje.replace(day=1)
        ate = hoje
    elif periodo == "Mês passado":
        primeiro_deste = hoje.replace(day=1)
        ate = primeiro_deste - timedelta(days=1)
        de  = ate.replace(day=1)

st.markdown('</div>', unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  PROCESSAR VENDAS
# ──────────────────────────────────────────────
def _processar_vendas(v, de, ate, inclui_estornos):
    if v.empty: return pd.DataFrame(), pd.DataFrame()
    v = v.copy(); v.columns = [c.strip() for c in v.columns]

    col_data  = _first_col(v, ["Data"])
    col_vid   = _first_col(v, ["VendaID","Pedido","Cupom"])
    col_idp   = _first_col(v, ["IDProduto","ProdutoID","ID"])
    col_qtd   = _first_col(v, ["Qtd","Quantidade","Qtde"])
    col_pu    = _first_col(v, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
    col_tot   = _first_col(v, ["TotalLinha","Total"])
    col_forma = _first_col(v, ["FormaPagto","FormaPagamento","Pagamento","Forma"])
    col_obs   = _first_col(v, ["Obs","Observação"])
    col_desc  = _first_col(v, ["Desconto"])
    col_totc  = _first_col(v, ["TotalCupom"])
    col_stat  = _first_col(v, ["CupomStatus","Status"])
    col_cli   = _first_col(v, ["Cliente"])

    out = pd.DataFrame({
        "Data":       v[col_data]  if col_data  else None,
        "VendaID":    v[col_vid]   if col_vid   else "",
        "IDProduto":  v[col_idp]   if col_idp   else None,
        "Qtd":        v[col_qtd]   if col_qtd   else 0,
        "PrecoUnit":  v[col_pu]    if col_pu    else 0,
        "TotalLinha": v[col_tot]   if col_tot   else 0,
        "Forma":      v[col_forma] if col_forma  else "",
        "Obs":        v[col_obs]   if col_obs   else "",
        "Desconto":   v[col_desc]  if col_desc  else 0,
        "TotalCupom": v[col_totc]  if col_totc  else None,
        "CupomStatus":v[col_stat]  if col_stat  else None,
        "Cliente":    v[col_cli]   if col_cli   else "",
    })
    out["Data_d"]    = out["Data"].apply(_parse_date)
    out = out[out["Data_d"].notna()]
    out = out[(out["Data_d"] >= de) & (out["Data_d"] <= ate)]

    out["QtdNum"]    = out["Qtd"].apply(_to_float)
    out["PrecoNum"]  = out["PrecoUnit"].apply(_to_float)
    out["TotalNum"]  = out["TotalLinha"].apply(_to_float)
    out["DescNum"]   = out["Desconto"].apply(_to_float)
    out["CupomNum"]  = out["TotalCupom"].apply(_to_float)
    out["VendaID"]   = out["VendaID"].astype(str).fillna("")
    out["IDProduto"] = out["IDProduto"].astype(str)
    out["is_estorno"]= out["VendaID"].str.startswith("CN-") | (out["CupomStatus"].astype(str).str.upper()=="ESTORNO")

    if not inclui_estornos:
        out = out[~out["is_estorno"]]

    cupom = out.groupby("VendaID", dropna=True).agg(
        Data_d=("Data_d","first"), Forma=("Forma","first"),
        TotalNum=("TotalNum","sum"), DescNum=("DescNum","max"),
        CupomNum=("CupomNum","max"), Cliente=("Cliente","first")
    ).reset_index()
    cupom["ReceitaCupom"] = cupom.apply(
        lambda r: r["CupomNum"] if r["CupomNum"] > 0 else max(0.0, r["TotalNum"] - r["DescNum"]), axis=1
    )
    return out, cupom

vendas, cupom = _processar_vendas(vend_raw, de, ate, inclui_estornos)


# ──────────────────────────────────────────────
#  CUSTO MÉDIO
# ──────────────────────────────────────────────
def _custo_map(comp_df, prod_df):
    mp = {}
    if not comp_df.empty:
        c = comp_df.copy(); c.columns = [x.strip() for x in c.columns]
        c_pid = _first_col(c, ["IDProduto","ProdutoID","ID"])
        c_qtd = _first_col(c, ["Qtd","Quantidade"])
        c_cu  = _first_col(c, ["Custo Unitário","CustoUnit","Custo"])
        if c_pid and c_qtd and c_cu:
            c["_q"] = c[c_qtd].apply(_to_float)
            c["_c"] = c[c_cu].apply(_to_float)
            c["_p"] = c["_q"] * c["_c"]
            g = c.groupby(c[c_pid].astype(str))[["_p","_q"]].sum()
            g["cm"] = g["_p"] / g["_q"].replace(0, pd.NA)
            mp = g["cm"].fillna(0.0).to_dict()
    if not prod_df.empty and "ID" in prod_df.columns and "CustoAtual" in prod_df.columns:
        for pid, custo in prod_df.set_index("ID")["CustoAtual"].items():
            if pid not in mp or mp[pid] == 0:
                try: mp[str(pid)] = float(custo) if pd.notna(custo) else 0.0
                except: pass
    return mp

custo_mp = _custo_map(comp_raw, prod)


# ──────────────────────────────────────────────
#  KPIs
# ──────────────────────────────────────────────
if vendas.empty:
    cupons = itens = receita = bruto = desc_tot = cogs = lucro = margem = 0
    cupons = 0; itens = 0; receita = 0.0; bruto = 0.0; desc_tot = 0.0
else:
    cupons   = cupom["VendaID"].nunique()
    itens    = int(vendas["QtdNum"].sum())
    receita  = cupom["ReceitaCupom"].sum()
    bruto    = cupom["TotalNum"].sum()
    desc_tot = max(0.0, bruto - receita)

cogs  = 0.0
if not vendas.empty and custo_mp:
    cogs = (vendas["QtdNum"] * vendas["IDProduto"].map(
        lambda x: float(custo_mp.get(str(x), 0.0)))).sum()

lucro  = max(0.0, receita - cogs)
margem = (lucro / receita * 100) if receita > 0 else 0.0

# Ticket médio
ticket = (receita / cupons) if cupons > 0 else 0.0

# Renderizar KPI cards
k1, k2, k3, k4, k5 = st.columns(5)

def _kpi(col, icon, label, value, sub="", color_class=""):
    col.markdown(f"""
    <div class="kpi-card">
      <span class="kpi-icon">{icon}</span>
      <div class="kpi-label">{label}</div>
      <div class="kpi-value {color_class}">{value}</div>
      {"<div class='kpi-sub'>" + sub + "</div>" if sub else ""}
    </div>
    """, unsafe_allow_html=True)

_kpi(k1, "🧾", "Cupons / Vendas",  str(cupons))
_kpi(k2, "📦", "Itens vendidos",   str(itens))
_kpi(k3, "💰", "Faturamento líq.", _brl(receita),
     f"Bruto {_brl(bruto)}" + (f" · Desc {_brl(desc_tot)}" if desc_tot > 0 else ""), "kpi-green")
_kpi(k4, "📈", "Lucro bruto est.", _brl(lucro),
     f"{margem:.1f}% de margem", "kpi-blue")
_kpi(k5, "🎯", "Ticket médio",     _brl(ticket), f"{cupons} cupons", "kpi-yellow")

st.markdown(f"""
<div class="info-caption">
  📅 {de.strftime('%d/%m/%Y')} a {ate.strftime('%d/%m/%Y')}
  &nbsp;·&nbsp; Estornos: {"✅ incluídos" if inclui_estornos else "❌ excluídos"}
  {"&nbsp;·&nbsp; Descontos: " + _brl(desc_tot) if desc_tot > 0 else ""}
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  LAYOUT: gráficos à esquerda, detalhes à direita
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">💳 Por forma de pagamento</div>', unsafe_allow_html=True)

if cupom.empty:
    st.info("Sem vendas no período selecionado.")
else:
    col_graf, col_det = st.columns([1.4, 1], gap="large")

    with col_graf:
        fpg = (cupom.groupby("Forma", dropna=False)["ReceitaCupom"]
               .sum().reset_index()
               .sort_values("ReceitaCupom", ascending=False))
        fpg["Forma"] = fpg["Forma"].fillna("—")

        # Cores por forma
        _cores = {"Dinheiro":"#4ade80","Pix":"#60a5fa","Cartão Débito":"#a78bfa",
                  "Cartão Crédito":"#f472b6","Fiado":"#fbbf24","Outros":"#94a3b8"}
        cores = [_cores.get(f, "#94a3b8") for f in fpg["Forma"]]

        fig = go.Figure(go.Bar(
            x=fpg["Forma"], y=fpg["ReceitaCupom"],
            marker_color=cores,
            text=[_brl(v) for v in fpg["ReceitaCupom"]],
            textposition="outside",
            textfont=dict(color="white", size=11),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white", family="DM Sans"),
            xaxis=dict(showgrid=False, color="rgba(255,255,255,0.5)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)",
                       color="rgba(255,255,255,0.3)", tickprefix="R$ "),
            margin=dict(l=0, r=0, t=16, b=0), height=260,
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_det:
        st.markdown("<br>", unsafe_allow_html=True)
        total_geral = fpg["ReceitaCupom"].sum()
        for _, row in fpg.iterrows():
            forma  = str(row["Forma"])
            valor  = row["ReceitaCupom"]
            pct    = (valor / total_geral * 100) if total_geral > 0 else 0
            emoji  = _forma_emoji(forma)
            n_cup  = len(cupom[cupom["Forma"] == forma])
            st.markdown(f"""
            <div class="forma-pill" style="width:100%;display:flex;justify-content:space-between;margin:0 0 8px 0;padding:12px 16px">
              <span>{emoji} {forma}</span>
              <span>
                <span class="val">{_brl(valor)}</span>
                <span style="color:rgba(255,255,255,0.3);font-size:0.72rem;margin-left:6px">{pct:.0f}% · {n_cup} {"cupom" if n_cup==1 else "cupons"}</span>
              </span>
            </div>
            """, unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  EVOLUÇÃO DIÁRIA (se período > 1 dia)
# ──────────────────────────────────────────────
if not cupom.empty and de != ate:
    st.markdown('<div class="sec-titulo">📈 Evolução diária</div>', unsafe_allow_html=True)

    diario = (cupom.groupby("Data_d")["ReceitaCupom"]
              .sum().reset_index()
              .sort_values("Data_d"))
    diario["Data_str"] = diario["Data_d"].apply(lambda d: d.strftime("%d/%m") if d else "")

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=diario["Data_str"], y=diario["ReceitaCupom"],
        mode="lines+markers",
        line=dict(color="#4ade80", width=2.5),
        marker=dict(color="#4ade80", size=7),
        fill="tozeroy",
        fillcolor="rgba(74,222,128,0.08)",
        text=[_brl(v) for v in diario["ReceitaCupom"]],
        hovertemplate="%{x}: %{text}<extra></extra>",
    ))
    fig2.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white", family="DM Sans"),
        xaxis=dict(showgrid=False, color="rgba(255,255,255,0.4)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)",
                   color="rgba(255,255,255,0.3)", tickprefix="R$ "),
        margin=dict(l=0, r=0, t=10, b=0), height=220,
    )
    st.plotly_chart(fig2, use_container_width=True)


# ──────────────────────────────────────────────
#  RESUMO POR PRODUTO
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">📦 Produtos mais vendidos</div>', unsafe_allow_html=True)

if vendas.empty:
    st.info("Sem vendas para detalhar.")
else:
    cup_map_df = cupom[["VendaID","ReceitaCupom","TotalNum"]].rename(columns={"TotalNum":"BrutoCupom"})
    tmp = vendas.merge(cup_map_df, how="left", on="VendaID")
    tmp["ReceitaLinha"] = tmp.apply(
        lambda r: r["TotalNum"] * (r["ReceitaCupom"] / r["BrutoCupom"])
        if r.get("BrutoCupom", 0) > 0 else r["TotalNum"], axis=1
    )

    key = "IDProduto"
    grp = (tmp[[key,"QtdNum","ReceitaLinha","TotalNum"]]
           .groupby(key, dropna=False)
           .agg(Qtd=("QtdNum","sum"), Receita=("ReceitaLinha","sum"),
                ReceitaBruta=("TotalNum","sum"))
           .reset_index())

    grp["COGS"]  = grp.apply(lambda r: r["Qtd"] * float(custo_mp.get(str(r[key]), 0.0)), axis=1)
    grp["Lucro"] = grp["Receita"] - grp["COGS"]

    if not prod.empty and {"ID","Nome"}.issubset(prod.columns):
        grp = grp.merge(prod[["ID","Nome"]].rename(columns={"ID":key}), how="left", on=key)
        grp["Produto"] = grp["Nome"].fillna(grp[key])
    else:
        grp["Produto"] = grp[key]

    grp = grp.sort_values("Receita", ascending=False).reset_index(drop=True)

    # Mini tabs: top produtos / tabela completa
    tab1, tab2 = st.tabs(["🏆 Top 10", "📋 Tabela completa"])

    with tab1:
        top = grp.head(10)
        receita_max = top["Receita"].max() if not top.empty else 1

        for _, row in top.iterrows():
            pct_barra = (row["Receita"] / receita_max * 100) if receita_max > 0 else 0
            lucro_color = "num-green" if row["Lucro"] >= 0 else "num-red"
            margem_p = (row["Lucro"] / row["Receita"] * 100) if row["Receita"] > 0 else 0

            st.markdown(f"""
            <div class="prod-row">
              <div class="prod-nome">
                {row['Produto']}
                <div style="background:rgba(74,222,128,0.15);border-radius:4px;height:4px;margin-top:5px;width:{pct_barra:.0f}%"></div>
              </div>
              <div class="prod-num num-gray">{int(row['Qtd'])} un</div>
              <div class="prod-num kpi-green">{_brl(row['Receita'])}</div>
              <div class="prod-num {lucro_color}">{_brl(row['Lucro'])}<br><span style="font-size:0.65rem;color:rgba(255,255,255,0.3)">{margem_p:.0f}% mg</span></div>
            </div>
            """, unsafe_allow_html=True)

    with tab2:
        # Formatar para exibição
        df_show = grp[["Produto","Qtd","Receita","COGS","Lucro"]].copy()
        df_show["Qtd"]     = df_show["Qtd"].apply(lambda x: f"{int(x)}")
        df_show["Receita"] = df_show["Receita"].apply(_brl)
        df_show["COGS"]    = df_show["COGS"].apply(_brl)
        df_show["Lucro"]   = df_show["Lucro"].apply(_brl)
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        st.download_button(
            "⬇️ Exportar CSV",
            grp[["Produto","Qtd","Receita","COGS","Lucro"]].to_csv(index=False).encode("utf-8"),
            file_name=f"fechamento_{de:%Y%m%d}_{ate:%Y%m%d}.csv",
            mime="text/csv",
            use_container_width=True,
        )
