# pages/05_fechamento_caixa.py — Fechamento de caixa
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Fechamento de caixa", page_icon="💵", layout="wide")
st.title("💵 Fechamento de caixa")

# ------- Helpers -------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=20)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, candidates) -> str | None:
    if df is None or df.empty: return None
    for c in candidates:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _to_num(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    s = s.replace(".", "").replace(",", ".") if s.count(",")==1 and s.count(".")>1 else s.replace(",", ".")
    try: return float(s)
    except: return 0.0

# ------- Carrega Vendas, Produtos -------
try:
    vend = carregar_aba("Vendas")
except Exception:
    vend = pd.DataFrame()

try:
    prod = carregar_aba("Produtos")
except Exception:
    prod = pd.DataFrame()

if vend.empty:
    st.info("Sem vendas para exibir.")
    st.stop()

col_data  = _first_col(vend, ["Data"])
col_id    = _first_col(vend, ["IDProduto","ProdutoID","ID"])
col_qtd   = _first_col(vend, ["Qtd","Quantidade","Qtde","Qde"])
col_preco = _first_col(vend, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
col_forma = _first_col(vend, ["FormaPagto","FormaPagamento","Pagamento","Forma"])
col_venda = _first_col(vend, ["VendaID","Pedido","Cupom"])

# parse datas (dd/mm/yyyy)
def _parse_date(s):
    try: return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
    except: return None
vend["_d"] = vend[col_data].map(_parse_date)

# Filtro de período
hoje = datetime.now().date()
c1, c2 = st.columns(2)
with c1: dt_ini = st.date_input("De", value=hoje)
with c2: dt_fim = st.date_input("Até", value=hoje)
mask = vend["_d"].between(dt_ini, dt_fim)
vendp = vend[mask].copy()

if vendp.empty:
    st.warning("Sem vendas no período selecionado."); st.stop()

# números
vendp["QtdNum"]   = vendp[col_qtd].map(_to_num)
vendp["PrecoNum"] = vendp[col_preco].map(_to_num) if col_preco else 0.0
vendp["TotalLinhaNum"] = vendp["QtdNum"] * (vendp["PrecoNum"] if col_preco else 0.0)

# fallback de valor se não houver PrecoUnit
if col_preco is None and not prod.empty:
    # tentar preço da aba Produtos
    col_pid = _first_col(prod, ["ID","Codigo","Código","SKU"])
    col_pnome = _first_col(prod, ["Nome","Produto"])
    col_pvenda = _first_col(prod, ["PrecoVenda","PreçoVenda","Preço","Preco"])
    if col_pid and col_pvenda:
        m = prod[[col_pid, col_pvenda]].copy()
        m.columns = ["IDProduto","PrecoPadrao"]
        vendp = vendp.merge(m, how="left", left_on=col_id, right_on="IDProduto")
        vendp["TotalLinhaNum"] = vendp["QtdNum"] * vendp["PrecoPadrao"].map(_to_num)

# custo estimado (para lucro bruto) via Produtos.CustoAtual
lucro_bruto = None
if not prod.empty:
    col_pid = _first_col(prod, ["ID","Codigo","Código","SKU"])
    col_custo = _first_col(prod, ["CustoAtual","Custo Médio","CustoMedio"])
    if col_pid and col_custo:
        m2 = prod[[col_pid, col_custo]].copy()
        m2.columns = ["IDProduto","CustoAtual"]
        vendp = vendp.merge(m2, how="left", left_on=col_id, right_on="IDProduto")
        vendp["CustoLinhaNum"] = vendp["QtdNum"] * vendp["CustoAtual"].map(_to_num)
        lucro_bruto = (vendp["TotalLinhaNum"].fillna(0) - vendp["CustoLinhaNum"].fillna(0)).sum()

# KPIs
total_itens = int(vendp["QtdNum"].sum())
faturamento = float(vendp["TotalLinhaNum"].sum())
num_cupons = vendp[col_venda].nunique() if col_venda else len(vendp.groupby([col_data, col_forma]))

k1,k2,k3,k4 = st.columns(4)
k1.metric("Cupons (vendas)", num_cupons)
k2.metric("Itens vendidos", total_itens)
k3.metric("Faturamento bruto", f"R$ {faturamento:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
if lucro_bruto is not None:
    k4.metric("Lucro bruto (estimado)", f"R$ {lucro_bruto:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
else:
    k4.metric("Lucro bruto (estimado)", "—")

st.divider()

# Por forma de pagamento
if col_forma:
    st.subheader("Por forma de pagamento")
    grp = vendp.groupby(col_forma, dropna=False)["TotalLinhaNum"].sum().reset_index().sort_values("TotalLinhaNum", ascending=False)
    if not grp.empty:
        fig = px.bar(grp, x=col_forma, y="TotalLinhaNum")
        fig.update_layout(xaxis_title="", yaxis_title="R$")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(grp.rename(columns={"TotalLinhaNum":"Valor (R$)"}), use_container_width=True, hide_index=True)
    else:
        st.info("Sem dados de pagamento.")

st.divider()

# Itens mais vendidos
st.subheader("Itens mais vendidos")
if not prod.empty:
    col_pid = _first_col(prod, ["ID","Codigo","Código","SKU"])
    col_pnome = _first_col(prod, ["Nome","Produto"])
    mp = prod[[col_pid, col_pnome]].copy()
    mp.columns = ["IDProduto","Nome"]
    top = vendp.groupby(col_id, dropna=False)["QtdNum"].sum().reset_index().rename(columns={"QtdNum":"Qtd"})
    top = top.merge(mp, how="left", left_on=col_id, right_on="IDProduto")
    top["Nome"] = top["Nome"].fillna(top[col_id])
    top = top.sort_values("Qtd", ascending=False).head(15)
    st.dataframe(top[["Nome","Qtd"]], use_container_width=True, hide_index=True)
else:
    top = vendp.groupby(col_id, dropna=False)["QtdNum"].sum().reset_index().rename(columns={"QtdNum":"Qtd"})
    st.dataframe(top, use_container_width=True, hide_index=True)

# Exportar CSV
csv = vendp.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇️ Exportar CSV (detalhado)", data=csv, file_name="fechamento.csv", mime="text/csv")
