# pages/30_teste.py — Dashboard (robusto) + Fiado
# -*- coding: utf-8 -*-

import json, unicodedata, re
from datetime import date, datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials


# ===========================
# Config da página
# ===========================
st.set_page_config(
    page_title="Teste — Ebenezér (robusto) + Fiado",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("🧪 Teste — Dashboard (robusto) + Fiado")


# ===========================
# Helpers secrets / auth
# ===========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key


def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc


@st.cache_resource(show_spinner=True)
def conectar_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)

    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente nos Secrets.")
        st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)


@st.cache_data(show_spinner=True)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = sh.worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df


# ===========================
# Utils
# ===========================
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")


def _norm_token(s: str) -> str:
    return re.sub(r"\s+", "", _strip_accents(s)).lower()


def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    # match direto (lower)
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    # match normalizado (sem acentos/espaços)
    normmap = {_norm_token(c): c for c in df.columns}
    for c in candidates:
        k = _norm_token(c)
        if k in normmap:
            return normmap[k]
    return None


def _to_float(x, default=0.0) -> float:
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except Exception:
        return default


def _parse_data_col(s):
    if pd.isna(s) or s is None:
        return None
    txt = str(s).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            pass
    try:
        d = pd.to_datetime(txt, dayfirst=True, errors="coerce")
        return None if pd.isna(d) else d.date()
    except Exception:
        return None


def _fmt_brl(v):
    try:
        return ("R$ " + f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


# ===========================
# Nomes de abas
# ===========================
ABA_PRODUTOS = "Produtos"
ABA_VENDAS   = "Vendas"
ABA_COMPRAS  = "Compras"

# Fiado — aceitamos múltiplos nomes
FIADO_ABAS_BASE = ["Fiado_Base", "Fiado Base", "Fiado", "Fiado_Lancamentos", "Fiado_Lançamentos", "Base de Dados"]
FIADO_ABAS_PAGT = ["Fiado_Pagamentos", "Fiado Pagamentos", "Pagamentos_Fiado", "Fiado_Pagos"]


# ===========================
# Carregar Produtos
# ===========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Não consegui abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

dfp = df_prod.copy()
dfp.columns = [c.strip() for c in dfp.columns]
# renomeações úteis
ren = {
    "PreçoVenda":"PrecoVenda", "Preço Venda":"PrecoVenda",
    "EstoqueAtual":"EstoqueAtual", "Estoque Min":"EstoqueMin", "Estoque Mínimo":"EstoqueMin",
}
for k,v in ren.items():
    if k in dfp.columns:
        dfp.rename(columns={k:v}, inplace=True)

if "EstoqueAtual" not in dfp.columns:
    dfp["EstoqueAtual"] = pd.to_numeric(dfp.get("EstoqueAtual", 0), errors="coerce").fillna(0)

# ===========================
# Filtros (sidebar)
# ===========================
st.sidebar.header("Filtros")

preset = st.sidebar.selectbox(
    "Período",
    ["Hoje", "Últimos 7 dias", "Últimos 30 dias", "Mês atual", "Personalizado"],
    index=2
)

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

cat_sel  = st.sidebar.multiselect("Categoria", sorted([x for x in dfp.get("Categoria", "").dropna().astype(str).unique()]))
forn_sel = st.sidebar.multiselect("Fornecedor", sorted([x for x in dfp.get("Fornecedor","").dropna().astype(str).unique()]))
apenas_ativos = st.sidebar.checkbox("Somente ativos", value=True)
busca = st.sidebar.text_input("Buscar por nome/ID")


# ===========================
# Carregar Vendas (ROBUSTO)
# ===========================
def _load_vendas_periodo():
    try:
        v = carregar_aba(ABA_VENDAS)
    except Exception:
        return pd.DataFrame()

    if v.empty:
        return v

    v.columns = [c.strip() for c in v.columns]

    col_data   = _first_col(v, ["Data"])
    col_qtd    = _first_col(v, ["Qtd", "Quantidade", "Qtde", "Qde"])
    col_preco  = _first_col(v, ["Preço Unitário", "Preco Unitário", "PreçoUnitário", "PrecoUnitario",
                                "Preço", "Preco", "PreçoVenda", "PrecoVenda"])
    col_total  = _first_col(v, ["Total", "Total (R$)", "TotalVenda", "Valor Total", "ValorTotal"])
    col_idprod = _first_col(v, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
    col_nome   = _first_col(v, ["Produto", "Nome", "Descrição"])
    col_forma  = _first_col(v, ["Forma Pagamento", "Forma de Pagamento", "Forma", "Pagamento", "Meio"])

    # datas
    if col_data:
        v["Data_d"] = v[col_data].apply(_parse_data_col)
        v = v[(v["Data_d"] >= dt_ini) & (v["Data_d"] <= dt_fim)]

    # qtd
    if col_qtd:
        v["Qtd_num"] = v[col_qtd].apply(lambda x: _to_float(x, 0))
    else:
        v["Qtd_num"] = 0.0

    # preço
    if col_preco:
        v["Preco_num"] = v[col_preco].apply(lambda x: _to_float(x, 0))
    else:
        # tenta pegar preço da aba Produtos (por ID, senão por nome)
        v["Preco_num"] = 0.0
        pv = _first_col(dfp, ["PrecoVenda"])
        if pv:
            if col_idprod and _first_col(dfp, ["ID"]):
                mapa = dfp.set_index("ID")[pv].apply(lambda x: _to_float(x, 0)).to_dict()
                v["Preco_num"] = v[col_idprod].map(mapa).fillna(0.0)
            elif col_nome and _first_col(dfp, ["Nome"]):
                mapa = dfp.set_index(_first_col(dfp, ["Nome"]))[pv].apply(lambda x: _to_float(x, 0)).to_dict()
                v["Preco_num"] = v[col_nome].map(mapa).fillna(0.0)

    # total
    if col_total:
        v["Total_num"] = v[col_total].apply(lambda x: _to_float(x, 0))
    else:
        v["Total_num"] = 0.0

    # fallback p/ total
    mask_calc = (v["Total_num"].isna()) | (v["Total_num"] == 0)
    v.loc[mask_calc, "Total_num"] = (v.loc[mask_calc, "Qtd_num"].fillna(0) * v.loc[mask_calc, "Preco_num"].fillna(0))

    # forma pagamento (para separar à vista vs “fiado” se existir)
    v["Forma_txt"] = v[col_forma].astype(str) if col_forma else ""

    # normaliza campos úteis
    v["IDProduto"] = v[col_idprod] if col_idprod else v.get("IDProduto", "")
    v["Produto"]   = v[col_nome]   if col_nome   else v.get("Produto", "")
    return v


# ===========================
# Carregar Compras (simples)
# ===========================
def _load_compras_periodo():
    try:
        c = carregar_aba(ABA_COMPRAS)
    except Exception:
        return pd.DataFrame()

    if c.empty:
        return c

    c.columns = [c.strip() for c in c.columns]
    col_data = _first_col(c, ["Data"])
    col_qtd  = _first_col(c, ["Qtd", "Quantidade", "Qtde", "Qde"])
    col_cu   = _first_col(c, ["Custo Unitário", "CustoUnitário", "CustoUnit", "Custo Unit", "Custo"])
    col_tot  = _first_col(c, ["Total", "Total (R$)", "Valor Total", "ValorTotal"])

    if col_data:
        c["Data_d"] = c[col_data].apply(_parse_data_col)
        c = c[(c["Data_d"] >= dt_ini) & (c["Data_d"] <= dt_fim)]

    c["Qtd_num"]   = c[col_qtd].apply(lambda x: _to_float(x, 0)) if col_qtd else 0.0
    c["Custo_num"] = c[col_cu].apply(lambda x: _to_float(x, 0)) if col_cu else 0.0
    c["Total_num"] = c[col_tot].apply(lambda x: _to_float(x, 0)) if col_tot else 0.0

    mask_calc = (c["Total_num"].isna()) | (c["Total_num"] == 0)
    c.loc[mask_calc, "Total_num"] = (c.loc[mask_calc, "Qtd_num"].fillna(0) *
                                     c.loc[mask_calc, "Custo_num"].fillna(0))
    return c


# ===========================
# FIADO — base & pagamentos
# ===========================
def _load_fiado_base():
    sh = conectar_sheets()
    # procura a primeira aba que existir com um dos nomes acima
    for nome in FIADO_ABAS_BASE:
        try:
            df = get_as_dataframe(sh.worksheet(nome), evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
            df.columns = [c.strip() for c in df.columns]
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _load_fiado_pagtos():
    sh = conectar_sheets()
    for nome in FIADO_ABAS_PAGT:
        try:
            df = get_as_dataframe(sh.worksheet(nome), evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
            df.columns = [c.strip() for c in df.columns]
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _fiado_saldo_aberto(df_base: pd.DataFrame) -> float:
    if df_base.empty:
        return 0.0
    col_status = _first_col(df_base, ["StatusFiado", "Status", "Situacao", "Situação"])
    col_valor  = _first_col(df_base, ["Valor", "Total", "Preço", "Preco"])
    if not col_valor:
        return 0.0
    if not col_status:
        # sem coluna de status: considera tudo como aberto
        return float(pd.to_numeric(df_base[col_valor].apply(_to_float), errors="coerce").fillna(0).sum())
    abertos = df_base[df_base[col_status].astype(str).str.lower().isin(["em aberto", "aberto", "pendente", "em aberto "])]
    return float(pd.to_numeric(abertos[col_valor].apply(_to_float), errors="coerce").fillna(0).sum())


def _fiado_recebido_periodo(df_pagt: pd.DataFrame) -> float:
    if df_pagt.empty:
        return 0.0
    col_data = _first_col(df_pagt, ["DataPagamento", "Data Pagamento", "Data", "Pago em"])
    col_liq  = _first_col(df_pagt, ["TotalLiquido", "Total Líquido", "Liquido", "ValorLiquido"])
    if not col_data or not col_liq:
        return 0.0
    df_pagt["Data_d"] = df_pagt[col_data].apply(_parse_data_col)
    df_p = df_pagt[(df_pagt["Data_d"] >= dt_ini) & (df_pagt["Data_d"] <= dt_fim)]
    return float(pd.to_numeric(df_p[col_liq].apply(_to_float), errors="coerce").fillna(0).sum())


# ===========================
# Carregamentos
# ===========================
vendas  = _load_vendas_periodo()
compras = _load_compras_periodo()
fiado_base = _load_fiado_base()
fiado_pagt = _load_fiado_pagtos()

# ===========================
# Métricas principais
# ===========================
faturamento = float(vendas["Total_num"].sum()) if not vendas.empty else 0.0
itens_vendidos = float(vendas["Qtd_num"].sum()) if not vendas.empty else 0.0
compras_total = float(compras["Total_num"].sum()) if not compras.empty else 0.0

# Vendas à vista:
# - Se existir coluna de forma pgto e houver algum registro contendo "fiado", exclui esses.
# - Caso contrário, considera tudo como à vista (mais simples para sua cliente).
avista = faturamento
if not vendas.empty:
    tem_forma = "Forma_txt" in vendas.columns
    if tem_forma:
        mask_fiado = vendas["Forma_txt"].str.lower().str.contains("fiado", na=False)
        avista = float(vendas.loc[~mask_fiado, "Total_num"].sum())

# COGS aprox: Qtd vendida × CustoAtual do produto
cogs = 0.0
if not vendas.empty:
    custo_col = _first_col(dfp, ["CustoAtual", "Custo Médio", "CustoMedio"])
    id_col_prod = _first_col(dfp, ["ID"])
    if custo_col and id_col_prod and "IDProduto" in vendas.columns:
        cmap = dfp.set_index(id_col_prod)[custo_col].apply(_to_float).to_dict()
        cogs = float((vendas["IDProduto"].map(cmap).fillna(0.0) * vendas["Qtd_num"].fillna(0.0)).sum())
    else:
        # fallback por nome
        nome_prod = _first_col(dfp, ["Nome", "Produto", "Descrição"])
        if custo_col and nome_prod and "Produto" in vendas.columns:
            cmap = dfp.set_index(nome_prod)[custo_col].apply(_to_float).to_dict()
            cogs = float((vendas["Produto"].map(cmap).fillna(0.0) * vendas["Qtd_num"].fillna(0.0)).sum())

lucro_bruto = max(0.0, faturamento - cogs)
margem_bruta = (lucro_bruto / faturamento * 100.0) if faturamento > 0 else 0.0

# Fiado: recebido no período + saldo aberto
fiado_recebido = _fiado_recebido_periodo(fiado_pagt)
fiado_aberto   = _fiado_saldo_aberto(fiado_base)

# Caixa do período (simplificado):
#   caixa = vendas à vista + fiado recebido - compras
caixa = avista + fiado_recebido - compras_total

# ===========================
# KPIs (cards)
# ===========================
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("🟩 Vendas no período (bruto)", _fmt_brl(faturamento))
k2.metric("🧾 Compras no período", _fmt_brl(compras_total))
k3.metric("📈 Lucro Bruto (aprox.)", _fmt_brl(lucro_bruto), f"{margem_bruta:.1f}% margem")
k4.metric("🛒 Itens vendidos", f"{itens_vendidos:.0f}", f"Ticket {_fmt_brl((faturamento/itens_vendidos) if itens_vendidos>0 else 0)}")
k5.metric("💼 Caixa do período", _fmt_brl(caixa))

k6,k7,k8 = st.columns(3)
k6.metric("🪙 Vendas à vista (período)", _fmt_brl(avista))
k7.metric("🏦 Recebido de fiado (período)", _fmt_brl(fiado_recebido))
k8.metric("📌 Fiado em aberto (saldo)", _fmt_brl(fiado_aberto))

st.caption(f"🗓️ Período: {dt_ini.strftime('%d/%m/%Y')} → {dt_fim.strftime('%d/%m/%Y')}")

st.divider()

# ===========================
# Vendas vs Compras por dia
# ===========================
st.subheader("📆 Vendas vs Compras por dia (período)")

def _group_daily(df_in, value_col, label):
    if df_in is None or df_in.empty:
        return pd.DataFrame(columns=["Data","Valor","Tipo"])
    tmp = df_in.copy()
    col = "Data_d" if "Data_d" in tmp.columns else _first_col(tmp, ["Data"])
    if col is None:
        return pd.DataFrame(columns=["Data","Valor","Tipo"])
    tmp[col] = tmp[col].apply(_parse_data_col)
    out = tmp.groupby(col)[value_col].sum().reset_index().rename(columns={value_col:"Valor", col:"Data"})
    out["Tipo"] = label
    return out

g_v = _group_daily(vendas, "Total_num", "Vendas")
g_c = _group_daily(compras, "Total_num", "Compras")
serie = pd.concat([g_v, g_c], ignore_index=True)

if not serie.empty:
    fig = px.bar(serie, x="Data", y="Valor", color="Tipo", barmode="group")
    fig.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados para o gráfico no período selecionado.")

st.divider()

# ===========================
# Top produtos por faturamento (período)
# ===========================
st.subheader("🏆 Top produtos por faturamento (período)")
if not vendas.empty:
    keycol = "IDProduto" if "IDProduto" in vendas.columns else "Produto"
    show_cols = []
    if "Produto" in vendas.columns:
        show_cols = ["Produto"]
    vtop = (vendas.groupby(keycol, dropna=False)["Total_num"]
            .sum().reset_index().sort_values("Total_num", ascending=False).head(10))
    # incluir nome amigável na tabela à direita, se der
    if keycol == "IDProduto" and "Produto" in vendas.columns:
        aux = vendas[[ "IDProduto","Produto" ]].drop_duplicates()
        vtop = vtop.merge(aux, how="left", on="IDProduto")
        show_cols = ["Produto"]
    c1,c2 = st.columns([1.2,1])
    with c1:
        figt = px.bar(vtop, x=(show_cols[0] if show_cols else keycol), y="Total_num")
        figt.update_layout(yaxis_title="R$", xaxis_title="")
        st.plotly_chart(figt, use_container_width=True)
    with c2:
        tcols = [keycol] + (show_cols if show_cols else []) + ["Total_num"]
        st.dataframe(vtop[tcols].rename(columns={"Total_num":"Total (R$)"}),
                     use_container_width=True, hide_index=True)
else:
    st.info("Sem vendas no período.")

st.divider()

# ===========================
# Estoque — KPIs simples
# ===========================
st.subheader("📦 Estoque — visão geral")
# filtros rápidos
mask = pd.Series(True, index=dfp.index)
if cat_sel:
    mask &= dfp.get("Categoria","").astype(str).isin(cat_sel)
if forn_sel:
    mask &= dfp.get("Fornecedor","").astype(str).isin(forn_sel)
if apenas_ativos and "Ativo?" in dfp.columns:
    mask &= (dfp["Ativo?"].astype(str).str.lower().isin(["sim", "true", "1"]))
if busca.strip():
    s = busca.lower()
    mask &= dfp.apply(lambda r: s in " ".join([str(x).lower() for x in r.values]), axis=1)

dfv = dfp[mask].copy()
dfv["CustoAtual_num"] = pd.to_numeric(dfv.get("CustoAtual", 0).apply(_to_float), errors="coerce").fillna(0)
dfv["EstoqueAtual_num"] = pd.to_numeric(dfv.get("EstoqueAtual", 0).apply(_to_float), errors="coerce").fillna(0)
dfv["ValorEstoque"] = dfv["CustoAtual_num"] * dfv["EstoqueAtual_num"]

total_skus = len(dfv)
ativos = (dfv.get("Ativo?", "").astype(str).str.lower().isin(["sim","true","1"]).sum()
          if "Ativo?" in dfv.columns else total_skus)
valor_estoque = float(dfv["ValorEstoque"].sum())
estq_min_col = _first_col(dfp, ["EstoqueMin"])
abaixo_min = 0
if estq_min_col:
    em = pd.to_numeric(dfv[estq_min_col].apply(_to_float), errors="coerce").fillna(0)
    abaixo_min = int((dfv["EstoqueAtual_num"] <= em).sum())

k1,k2,k3,k4 = st.columns(4)
k1.metric("Produtos exibidos", f"{total_skus}")
k2.metric("Ativos (sim)", f"{ativos}")
k3.metric("💰 Valor em estoque", _fmt_brl(valor_estoque))
k4.metric("⚠️ Itens abaixo do mínimo", f"{abaixo_min}")

st.divider()

st.subheader("⚠️ Alerta de ruptura / Sugestão de compra")
if estq_min_col:
    df_alerta = dfv.copy()
    df_alerta["EstoqueMin_num"] = pd.to_numeric(df_alerta[estq_min_col].apply(_to_float), errors="coerce").fillna(0)
    df_alerta = df_alerta[df_alerta["EstoqueAtual_num"] <= df_alerta["EstoqueMin_num"]]
    df_alerta["SugestaoCompra"] = (df_alerta["EstoqueMin_num"]*2 - df_alerta["EstoqueAtual_num"]).clip(lower=0).round()
    cols_alerta = [c for c in ["ID","Nome","Categoria","Fornecedor","EstoqueAtual_num",estq_min_col,"SugestaoCompra"] if c in df_alerta.columns or c=="EstoqueAtual_num"]
    ren_cols = {"EstoqueAtual_num":"EstoqueAtual"}
    st.dataframe(df_alerta[cols_alerta].rename(columns=ren_cols), use_container_width=True, hide_index=True)
else:
    st.info("Sua aba Produtos não tem coluna de Estoque Mínimo, então não dá para sugerir compras.")

