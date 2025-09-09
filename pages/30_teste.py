# pages/30_teste.py — Dashboard (robusto a nomes de colunas) + Fiado integrado
# -*- coding: utf-8 -*-
import json, unicodedata, re
from collections.abc import Mapping
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Teste — Ebenezér Variedades", page_icon="🧪", layout="wide")
st.title("🧪 Teste — Dashboard (robusto) + Fiado")

# =========================
# Helpers p/ Secrets & Auth
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_service_account_from_secrets() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente em Settings → Secrets.")
        st.stop()
    if isinstance(svc, str):
        try:
            svc = json.loads(svc)
        except Exception as e:
            st.error("🛑 GCP_SERVICE_ACCOUNT é uma string, mas não é JSON válido.")
            st.caption(str(e)); st.stop()
    if not isinstance(svc, Mapping):
        st.error("🛑 GCP_SERVICE_ACCOUNT precisa ser um objeto JSON/TOML (tabela).")
        st.stop()
    req = ["type","project_id","private_key_id","private_key","client_email","token_uri"]
    miss = [k for k in req if k not in svc]
    if miss:
        st.error("🛑 Faltam campos no Service Account: " + ", ".join(miss)); st.stop()
    pk = str(svc["private_key"])
    if "BEGIN PRIVATE KEY" not in pk:
        st.error("🛑 private_key inválida. Cole a chave completa (BEGIN/END)."); st.stop()
    return {**svc, "private_key": _normalize_private_key(pk)}

@st.cache_resource(show_spinner=True)
def conectar_sheets():
    svc = _load_service_account_from_secrets()
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL não está no Secrets."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(show_spinner=True, ttl=20)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = sh.worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    return df

# =========================
# Utils — números & datas
# =========================
def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

def _parse_data_col(s):
    if pd.isna(s) or s is None: return None
    txt = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(txt, fmt).date()
        except: pass
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="coerce").date()
    except:
        return None

def _fmt_brl(v):
    try:
        return ("R$ " + f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _series_num(df: pd.DataFrame, col: str | None) -> pd.Series:
    if col and col in df.columns:
        return df[col].apply(lambda x: _to_float(x, 0.0))
    return pd.Series([0.0]*len(df), index=df.index, dtype=float)

# =========================
# Abas na planilha
# =========================
ABA_PRODUTOS    = "Produtos"
ABA_VENDAS      = "Vendas"
ABA_COMPRAS     = "Compras"
ABA_FIADO       = "Fiado"
ABA_FIADO_PAGT  = "Fiado_Pagamentos"

# =========================
# Produtos (base p/ estoque e custo)
# =========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Não consegui abrir a planilha de Produtos.")
    with st.expander("Detalhes técnicos"): st.code(str(e))
    st.stop()

if df_prod.empty:
    st.warning(f"A aba **{ABA_PRODUTOS}** está vazia."); st.stop()

df = df_prod.copy()
df.columns = [c.strip() for c in df.columns]
ren = {
    "ID":"ID","Nome":"Nome","Categoria":"Categoria","Unidade":"Unidade","Fornecedor":"Fornecedor",
    "CustoAtual":"CustoAtual","PreçoVenda":"PrecoVenda","Preço Venda":"PrecoVenda","PrecoVenda":"PrecoVenda",
    "Markup %":"MarkupPct","Margem %":"MargemPct",
    "EstoqueAtual":"EstoqueAtual","EstoqueMin":"EstoqueMin","LeadTimeDias":"LeadTimeDias","Ativo?":"Ativo"
}
for k,v in ren.items():
    if k in df.columns: df.rename(columns={k:v}, inplace=True)

for c in ["CustoAtual","PrecoVenda","MarkupPct","MargemPct","EstoqueAtual","EstoqueMin","LeadTimeDias"]:
    if c not in df.columns: df[c] = None
for c in ["Categoria","Fornecedor","Ativo","Nome","ID"]:
    if c not in df.columns: df[c] = None

num_cols = ["CustoAtual","PrecoVenda","MarkupPct","MargemPct","EstoqueAtual","EstoqueMin","LeadTimeDias"]
for c in num_cols: df[c] = pd.to_numeric(df[c], errors="coerce")
df["Ativo"] = df["Ativo"].astype(str).str.strip().str.lower()
df["Ativo"] = df["Ativo"].map({"sim":"sim","true":"sim","1":"sim"}).fillna(df["Ativo"])

df["MargemPct"] = df["MargemPct"].where(df["MargemPct"].notna(),
    ((df["PrecoVenda"] - df["CustoAtual"]) / df["PrecoVenda"] * 100))
df["MarkupPct"] = df["MarkupPct"].where(df["MarkupPct"].notna(),
    ((df["PrecoVenda"] / df["CustoAtual"] - 1) * 100))
df["ValorEstoque"] = (df["CustoAtual"].fillna(0) * df["EstoqueAtual"].fillna(0))
df["AbaixoMin"] = (df["EstoqueAtual"].fillna(0) <= df["EstoqueMin"].fillna(0))

# =========================
# Filtros (inclui período)
# =========================
st.sidebar.header("Filtros")
preset = st.sidebar.selectbox("Período",
    ["Hoje","Últimos 7 dias","Últimos 30 dias","Mês atual","Personalizado"], index=2)

hoje = date.today()
if preset == "Hoje":
    dt_ini, dt_fim = hoje, hoje
elif preset == "Últimos 7 dias":
    dt_ini, dt_fim = hoje - timedelta(days=6), hoje
elif preset == "Últimos 30 dias":
    dt_ini, dt_fim = hoje - timedelta(days=29), hoje
elif preset == "Mês atual":
    dt_ini, dt_fim = hoje.replace(day=1), hoje
else:
    c1, c2 = st.sidebar.columns(2)
    with c1: dt_ini = st.date_input("De:", value=hoje - timedelta(days=29))
    with c2: dt_fim = st.date_input("Até:", value=hoje)

cat_sel = st.sidebar.multiselect("Categoria", sorted([x for x in df["Categoria"].dropna().astype(str).unique()]))
forn_sel = st.sidebar.multiselect("Fornecedor", sorted([x for x in df["Fornecedor"].dropna().astype(str).unique()]))
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

# =========================
# Vendas & Compras — robusto a nomes
# =========================
def _load_vendas_periodo():
    try:
        v = carregar_aba(ABA_VENDAS)
    except Exception:
        return pd.DataFrame(columns=["Data","Produto","IDProduto","Qtd","Preço Unitário","Total","Forma Pagamento","Obs"])

    if v.empty: return v
    v.columns = [c.strip() for c in v.columns]

    data_col = _pick_col(v, ["Data","Dia","Data Venda","DataVenda"])
    if data_col:
        v["Data_d"] = v[data_col].apply(_parse_data_col)
        v = v[(v["Data_d"] >= dt_ini) & (v["Data_d"] <= dt_fim)]

    qtd_col   = _pick_col(v, ["Qtd","Quantidade","Qtde","Qde"])
    preco_col = _pick_col(v, [
        "Preço Unitário","Preco Unitário","PreçoUnitário","PrecoUnitario",
        "Preço","Preco","Valor Unitário","ValorUnitario","Unitário","Unitario"
    ])
    total_col = _pick_col(v, ["Total","Valor Total","Total Venda","TotalVenda","Valor"])

    v["Qtd_num"]   = _series_num(v, qtd_col)
    v["Preco_num"] = _series_num(v, preco_col)
    v["Total_num"] = _series_num(v, total_col)

    # Reconstruções
    m = (v["Total_num"] <= 0) & (v["Preco_num"] > 0) & (v["Qtd_num"] > 0)
    v.loc[m, "Total_num"] = v.loc[m, "Preco_num"] * v.loc[m, "Qtd_num"]

    m = (v["Preco_num"] <= 0) & (v["Total_num"] > 0) & (v["Qtd_num"] > 0)
    v.loc[m, "Preco_num"] = v.loc[m, "Total_num"] / v.loc[m, "Qtd_num"]

    m = (v["Qtd_num"] <= 0) & (v["Total_num"] > 0) & (v["Preco_num"] > 0)
    v.loc[m, "Qtd_num"] = v.loc[m, "Total_num"] / v.loc[m, "Preco_num"]

    return v

def _load_compras_periodo():
    try:
        c = carregar_aba(ABA_COMPRAS)
    except Exception:
        return pd.DataFrame(columns=["Data","Produto","Qtd","Custo Unitário","Total","IDProduto"])

    if c.empty: return c
    c.columns = [x.strip() for x in c.columns]

    data_col = _pick_col(c, ["Data","Dia","Data Compra","DataCompra"])
    if data_col:
        c["Data_d"] = c[data_col].apply(_parse_data_col)
        c = c[(c["Data_d"] >= dt_ini) & (c["Data_d"] <= dt_fim)]

    qtd_col   = _pick_col(c, ["Qtd","Quantidade","Qtde","Qde"])
    custo_col = _pick_col(c, [
        "Custo Unitário","CustoUnitário","Custo Unit","CustoUnit",
        "Custo","Preço","Preco","Valor Unitário","ValorUnitario"
    ])
    total_col = _pick_col(c, ["Total","Valor Total","Total Compra","TotalCompra","Valor"])

    c["Qtd_num"]   = _series_num(c, qtd_col)
    c["Custo_num"] = _series_num(c, custo_col)
    c["Total_num"] = _series_num(c, total_col)

    # Reconstruções
    m = (c["Total_num"] <= 0) & (c["Custo_num"] > 0) & (c["Qtd_num"] > 0)
    c.loc[m, "Total_num"] = c.loc[m, "Custo_num"] * c.loc[m, "Qtd_num"]

    m = (c["Custo_num"] <= 0) & (c["Total_num"] > 0) & (c["Qtd_num"] > 0)
    c.loc[m, "Custo_num"] = c.loc[m, "Total_num"] / c.loc[m, "Qtd_num"]

    m = (c["Qtd_num"] <= 0) & (c["Total_num"] > 0) & (c["Custo_num"] > 0)
    c.loc[m, "Qtd_num"] = c.loc[m, "Total_num"] / c.loc[m, "Custo_num"]

    return c

vendas_p  = _load_vendas_periodo()
compras_p = _load_compras_periodo()

# =========================
# FIADO — saldo aberto e recebimento do período
# =========================
def _load_fiado_periodo():
    try:
        f = carregar_aba(ABA_FIADO)
        f.columns = [c.strip() for c in f.columns]
        f = f.loc[:, ~pd.Index(f.columns).duplicated(keep="first")]
    except Exception:
        f = pd.DataFrame()

    try:
        fp = carregar_aba(ABA_FIADO_PAGT)
        fp.columns = [c.strip() for c in fp.columns]
        fp = fp.loc[:, ~pd.Index(fp.columns).duplicated(keep="first")]
    except Exception:
        fp = pd.DataFrame()

    if not f.empty:
        f["Status_norm"] = f.get("Status","").astype(str).str.strip().str.lower()
        f["Valor_num"]   = f.get("Valor","").apply(lambda x: _to_float(x, 0))
        fiado_aberto_total = float(f.loc[f["Status_norm"]=="em aberto","Valor_num"].sum())
    else:
        fiado_aberto_total = 0.0

    if not fp.empty:
        fp["Data_d"] = fp.get("DataPagamento","").apply(_parse_data_col)
        fp = fp[(fp["Data_d"]>=dt_ini) & (fp["Data_d"]<=dt_fim)]
        if "TotalPago" in fp.columns:
            fp["TotalPago_num"] = fp["TotalPago"].apply(lambda x: _to_float(x, 0))
        else:
            fp["TotalPago_num"] = fp.get("TotalLiquido", 0).apply(lambda x: _to_float(x, 0))
        fiado_recebido_periodo = float(fp["TotalPago_num"].sum())
    else:
        fiado_recebido_periodo = 0.0

    return f, fp, fiado_aberto_total, fiado_recebido_periodo

fiado_df, fiado_pag_df, fiado_aberto_total, fiado_recebido_periodo = _load_fiado_periodo()

# =========================
# KPIs & Caixa
# =========================
# COGS aproximado (Qtd × CustoAtual)
if not vendas_p.empty:
    # mapa de custo por ID ou Nome
    custo_map = dfv.set_index("ID")["CustoAtual"].to_dict() if "ID" in dfv.columns else {}
    if not custo_map and "Nome" in dfv.columns:
        custo_map = dfv.set_index("Nome")["CustoAtual"].to_dict()

    def _custo_aprox(row):
        key = row.get("IDProduto") or row.get("Produto")
        return (custo_map.get(key, 0) or 0) * row["Qtd_num"]

    vendas_p["COGS_aprox"] = vendas_p.apply(_custo_aprox, axis=1)
else:
    vendas_p = vendas_p.copy()
    vendas_p["COGS_aprox"] = pd.Series(dtype=float)

faturamento_total_vendas = float(vendas_p.get("Total_num", pd.Series(dtype=float)).sum())
itens_vendidos = float(vendas_p.get("Qtd_num", pd.Series(dtype=float)).sum())
compras_total  = float(compras_p.get("Total_num", pd.Series(dtype=float)).sum())
cogs           = float(vendas_p.get("COGS_aprox", pd.Series(dtype=float)).sum())

lucro_bruto  = max(0.0, faturamento_total_vendas - cogs)
margem_bruta = (lucro_bruto / faturamento_total_vendas * 100) if faturamento_total_vendas > 0 else 0.0
ticket_medio = (faturamento_total_vendas / itens_vendidos) if itens_vendidos > 0 else 0.0

# Vendas à vista (exclui 'Fiado' se existir a coluna de forma)
if not vendas_p.empty and "Forma Pagamento" in vendas_p.columns:
    mask_fiado_v = vendas_p["Forma Pagamento"].astype(str).str.strip().str.lower().eq("fiado")
    faturamento_avista = float(vendas_p.loc[~mask_fiado_v, "Total_num"].sum())
else:
    faturamento_avista = float(faturamento_total_vendas)

# Caixa do período: à vista + fiado recebido − compras
caixa_periodo = faturamento_avista + fiado_recebido_periodo - compras_total

# KPIs — linha 1
k1,k2,k3,k4 = st.columns(4)
k1.metric("💵 Vendas no período (bruto)", _fmt_brl(faturamento_total_vendas))
k2.metric("🧾 Compras no período", _fmt_brl(compras_total))
k3.metric("📈 Lucro Bruto (aprox.)", _fmt_brl(lucro_bruto), f"{margem_bruta:.1f}% margem")
k4.metric("🧮 Itens vendidos", f"{itens_vendidos:.0f}", f"Ticket {_fmt_brl(ticket_medio)}")

# KPIs — linha 2 (caixa + fiado)
c_ca1, c_ca2, c_ca3 = st.columns(3)
c_ca1.metric("💵 Vendas à vista (período)", _fmt_brl(faturamento_avista))
c_ca2.metric("💳 Recebido de fiado (período)", _fmt_brl(fiado_recebido_periodo))
c_ca3.metric("🧮 Caixa do período", _fmt_brl(caixa_periodo))

c_f1, c_f2 = st.columns(2)
c_f1.metric("🧾 Fiado em aberto (saldo)", _fmt_brl(fiado_aberto_total))
c_f2.metric("📅 Período", f"{dt_ini.strftime('%d/%m/%Y')} → {dt_fim.strftime('%d/%m/%Y')}")

st.divider()

# =========================
# Gráficos
# =========================
st.subheader("📆 Vendas vs Compras por dia (período)")
def _group_daily(df_in, value_col, label):
    if df_in.empty:
        return pd.DataFrame(columns=["Data","Valor","Tipo"])
    tmp = df_in.copy()
    col = "Data_d" if "Data_d" in tmp.columns else "Data"
    tmp[col] = tmp[col].apply(_parse_data_col)
    out = tmp.groupby(col)[value_col].sum().reset_index().rename(columns={value_col:"Valor", col:"Data"})
    out["Tipo"] = label
    return out

g_v = _group_daily(vendas_p, "Total_num", "Vendas")
g_c = _group_daily(compras_p, "Total_num", "Compras")
serie = pd.concat([g_v, g_c], ignore_index=True)

if not serie.empty:
    fig = px.bar(serie, x="Data", y="Valor", color="Tipo", barmode="group", title="")
    fig.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados no período selecionado.")

st.divider()

st.subheader("💳 Recebido de fiado por dia (período)")
if not fiado_pag_df.empty:
    g_f = (fiado_pag_df.groupby("Data_d")["TotalPago_num"]
           .sum().reset_index().rename(columns={"Data_d":"Data","TotalPago_num":"Valor"}))
    fig_f = px.bar(g_f, x="Data", y="Valor", title="")
    fig_f.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig_f, use_container_width=True)
else:
    st.info("Sem recebimentos de fiado no período.")

st.divider()

st.subheader("💳 Vendas por forma de pagamento")
if not vendas_p.empty and "Forma Pagamento" in vendas_p.columns:
    fp = vendas_p.groupby(vendas_p["Forma Pagamento"].astype(str))["Total_num"].sum().reset_index()
    col1,col2 = st.columns([1.1,1])
    with col1:
        fig_fp = px.bar(fp, x="Forma Pagamento", y="Total_num", title="")
        fig_fp.update_layout(yaxis_title="R$", xaxis_title="")
        st.plotly_chart(fig_fp, use_container_width=True)
    with col2:
        st.dataframe(fp.rename(columns={"Total_num":"Total (R$)"}), use_container_width=True, hide_index=True)
else:
    st.info("Sem vendas para detalhar por forma de pagamento.")

st.divider()

st.subheader("🏆 Top produtos por faturamento (período)")
if not vendas_p.empty:
    keycol = "IDProduto" if "IDProduto" in vendas_p.columns else "Produto"
    tmp = vendas_p.copy()
    if keycol == "IDProduto" and "Produto" not in tmp.columns:
        tmp["Produto"] = tmp["IDProduto"]
    vtop = (tmp.groupby([keycol, "Produto"], dropna=False)["Total_num"]
               .sum().reset_index()
               .sort_values("Total_num", ascending=False).head(10))
    c1,c2 = st.columns([1.2,1])
    with c1:
        figt = px.bar(vtop, x="Produto", y="Total_num")
        figt.update_layout(yaxis_title="R$", xaxis_title="")
        st.plotly_chart(figt, use_container_width=True)
    with c2:
        st.dataframe(vtop.rename(columns={"Total_num":"Total (R$)"}), use_container_width=True, hide_index=True)
else:
    st.info("Sem vendas no período.")

st.divider()

# =========================
# Estoque — KPIs e tabelas
# =========================
total_prod = len(dfv)
ativos = (dfv["Ativo"] == "sim").sum()
valor_estoque = float(dfv["ValorEstoque"].sum())
abaixo_min = int(dfv["AbaixoMin"].sum())

k1,k2,k3,k4 = st.columns(4)
k1.metric("Produtos exibidos", f"{total_prod}")
k2.metric("Ativos (sim)", f"{ativos}")
k3.metric("💰 Valor em estoque", _fmt_brl(valor_estoque))
k4.metric("⚠️ Itens abaixo do mínimo", f"{abaixo_min}")

st.divider()

st.subheader("⚠️ Alerta de ruptura / Sugestão de compra")
df_alerta = dfv[dfv["AbaixoMin"]].copy()
if not df_alerta.empty:
    df_alerta["SugestaoCompra"] = (df_alerta["EstoqueMin"].fillna(0)*2 - df_alerta["EstoqueAtual"].fillna(0)).clip(lower=0).round()
    cols_alerta = ["ID","Nome","Categoria","Fornecedor","EstoqueAtual","EstoqueMin","SugestaoCompra","LeadTimeDias"]
    st.dataframe(df_alerta[[c for c in cols_alerta if c in df_alerta.columns]],
                 use_container_width=True, hide_index=True)
else:
    st.success("Sem itens abaixo do mínimo 🎉")

st.divider()

st.subheader("🏆 Top 10 — Valor em estoque")
df_top = dfv.sort_values("ValorEstoque", ascending=False).head(10)
c1,c2 = st.columns([1.2,1])
with c1:
    if not df_top.empty:
        fig = px.bar(df_top, x="Nome", y="ValorEstoque",
                     hover_data=["EstoqueAtual","CustoAtual","Categoria"])
        fig.update_layout(xaxis_title="", yaxis_title="R$ em estoque")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para o gráfico (defina CustoAtual e EstoqueAtual em Produtos).")
with c2:
    st.caption("Tabela Top 10")
    cols = [c for c in ["ID","Nome","Categoria","EstoqueAtual","CustoAtual","ValorEstoque"] if c in df_top.columns]
    st.dataframe(df_top[cols] if cols else df_top, use_container_width=True, hide_index=True, height=420)

st.divider()

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
st.subheader("📋 Lista de produtos (filtrada)")
mostrar_cols = ["ID","Nome","Categoria","Fornecedor","CustoAtual","PrecoVenda","MargemPct","EstoqueAtual","EstoqueMin","ValorEstoque","Ativo"]
mostrar_cols = [c for c in mostrar_cols if c in dfv.columns]
st.dataframe(dfv[mostrar_cols] if mostrar_cols else dfv, use_container_width=True, hide_index=True)
