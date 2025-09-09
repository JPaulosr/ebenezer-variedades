# app.py — Dashboard Ebenezér Variedades (estoque/custo ao vivo + fix concat)
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

st.set_page_config(page_title="Ebenezér Variedades — Dashboard", page_icon="🧮", layout="wide")
st.title("🧮 Dashboard — Ebenezér Variedades")

# =========================
# Auth & Conexão
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    if not isinstance(svc, Mapping):
        st.error("🛑 GCP_SERVICE_ACCOUNT inválido."); st.stop()
    pk = str(svc.get("private_key",""))
    if "BEGIN PRIVATE KEY" not in pk:
        st.error("🛑 private_key inválida. Cole a chave completa (BEGIN/END)."); st.stop()
    svc = {**svc, "private_key": _normalize_private_key(pk)}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc     = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL não está no Secrets."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=20, show_spinner=False)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

# =========================
# Utils
# =========================
def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ","").replace("\u00A0","")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count(".") > 1:
        parts = s.split("."); s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

def _parse_date_any(s):
    if s is None or (isinstance(s, float) and pd.isna(s)): return None
    txt = str(s).strip()
    for fmt in ("%d/%m/%Y","%Y-%m-%d","%d/%m/%y"):
        try: return datetime.strptime(txt, fmt).date()
        except: pass
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="coerce").date()
    except: return None

def _first_col(df: pd.DataFrame, candidates) -> str | None:
    if df is None or df.empty: return None
    cols = list(df.columns)
    for c in candidates:
        if c in cols: return c
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _fmt_brl(v):
    try:
        return ("R$ " + f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except: return "R$ 0,00"

def _unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    """garante nomes de colunas únicos para evitar InvalidIndexError no concat"""
    if df is None or df.empty: 
        return df
    cols = []
    seen = {}
    for c in df.columns:
        base = str(c)
        if base not in seen:
            seen[base] = 1
            cols.append(base)
        else:
            seen[base] += 1
            cols.append(f"{base}__{seen[base]}")
    df.columns = cols
    return df

# =========================
# Carrega abas
# =========================
ABA_PROD, ABA_VEND, ABA_COMP = "Produtos", "Vendas", "Compras"

try:    prod = carregar_aba(ABA_PROD)
except: prod = pd.DataFrame()
try:    vend_raw = carregar_aba(ABA_VEND)
except: vend_raw = pd.DataFrame()
try:    comp_raw = carregar_aba(ABA_COMP)
except: comp_raw = pd.DataFrame()

# =========================
# Normalização de PRODUTOS (base estática)
# =========================
if prod.empty:
    st.warning("Aba Produtos está vazia.")
else:
    ren = {
        "ID":"ID","Nome":"Nome","Categoria":"Categoria","Unidade":"Unidade","Fornecedor":"Fornecedor",
        "CustoAtual":"CustoAtual","PreçoVenda":"PrecoVenda","Preço Venda":"PrecoVenda","PrecoVenda":"PrecoVenda",
        "Markup %":"MarkupPct","Margem %":"MargemPct",
        "EstoqueAtual":"EstoqueAtual","EstoqueMin":"EstoqueMin","LeadTimeDias":"LeadTimeDias","Ativo?":"Ativo"
    }
    for k,v in ren.items():
        if k in prod.columns and v!=k: prod.rename(columns={k:v}, inplace=True)
    for c in ["ID","Nome","Categoria","Fornecedor","EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda","Ativo"]:
        if c not in prod.columns: prod[c] = None
    for c in ["EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda"]:
        prod[c] = pd.to_numeric(prod[c], errors="coerce")
    prod["ValorEstoque"] = prod["CustoAtual"].fillna(0)*prod["EstoqueAtual"].fillna(0)

# =========================
# Filtros (período + cat/forn/ativos)
# =========================
st.sidebar.header("Filtros")
preset = st.sidebar.selectbox("Período", ["Hoje","Últimos 7 dias","Últimos 30 dias","Mês atual","Personalizado"], index=2)
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

inclui_estornos = st.sidebar.checkbox("Incluir estornos (CN-/ESTORNO)", value=False)

cats = sorted(pd.Series(prod["Categoria"].dropna().astype(str).unique()).tolist()) if not prod.empty else []
forns = sorted(pd.Series(prod["Fornecedor"].dropna().astype(str).unique()).tolist()) if not prod.empty else []
cat_sel  = st.sidebar.multiselect("Categoria", cats)
forn_sel = st.sidebar.multiselect("Fornecedor", forns)
apenas_ativos = st.sidebar.checkbox("Somente ativos", value=True)
busca = st.sidebar.text_input("Buscar por nome/ID")

# =========================
# Normalização VENDAS (período)
# =========================
def _normalize_vendas_period(v: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if v.empty:
        return pd.DataFrame(), pd.DataFrame()
    v = v.copy()
    v.columns = [c.strip() for c in v.columns]
    col_data  = _first_col(v, ["Data"])
    col_vid   = _first_col(v, ["VendaID","Pedido","Cupom"])
    col_idp   = _first_col(v, ["IDProduto","ProdutoID","ID"])
    col_qtd   = _first_col(v, ["Qtd","Quantidade","Qtde","Qde"])
    col_pu    = _first_col(v, ["PrecoUnit","Preço Unitário","PreçoUnitário","Preço","Preco"])
    col_tot   = _first_col(v, ["TotalLinha","Total"])
    col_forma = _first_col(v, ["FormaPagto","Forma Pagamento","FormaPagamento","Pagamento","Forma"])
    col_obs   = _first_col(v, ["Obs","Observação"])
    col_desc  = _first_col(v, ["Desconto"])
    col_totcup= _first_col(v, ["TotalCupom"])
    col_stat  = _first_col(v, ["CupomStatus","Status"])

    out = pd.DataFrame({
        "Data":      v[col_data]  if col_data else None,
        "VendaID":   v[col_vid]   if col_vid  else "",
        "IDProduto": v[col_idp]   if col_idp  else None,
        "Qtd":       v[col_qtd]   if col_qtd  else 0,
        "PrecoUnit": v[col_pu]    if col_pu   else 0,
        "TotalLinha":v[col_tot]   if col_tot  else 0,
        "Forma":     v[col_forma] if col_forma else "",
        "Obs":       v[col_obs]   if col_obs  else "",
        "Desconto":  v[col_desc]  if col_desc else 0,
        "TotalCupom":v[col_totcup]if col_totcup else None,
        "CupomStatus":v[col_stat] if col_stat else None
    })
    out["Data_d"]     = out["Data"].apply(_parse_date_any)
    out["QtdNum"]     = out["Qtd"].apply(_to_float)
    out["PrecoNum"]   = out["PrecoUnit"].apply(_to_float)
    out["TotalNum"]   = out["TotalLinha"].apply(_to_float)
    out["DescNum"]    = out["Desconto"].apply(_to_float)
    out["TotalCupomNum"] = out["TotalCupom"].apply(_to_float)
    out["VendaID"]    = out["VendaID"].astype(str).fillna("")
    out["is_estorno"] = out["VendaID"].str.startswith("CN-") | (out["CupomStatus"].astype(str).str.upper()=="ESTORNO")

    # Período
    out = out[(out["Data_d"]>=dt_ini) & (out["Data_d"]<=dt_fim)]
    if not inclui_estornos:
        out = out[~out["is_estorno"]]

    # Receita por cupom (respeita desconto)
    cupom_grp = out.groupby("VendaID", dropna=True).agg({
        "Data_d":"first","Forma":"first","TotalNum":"sum","DescNum":"max","TotalCupomNum":"max"
    }).reset_index()
    cupom_grp["ReceitaCupom"] = cupom_grp.apply(
        lambda r: r["TotalCupomNum"] if r["TotalCupomNum"]>0 else max(0.0, r["TotalNum"] - r["DescNum"]), axis=1
    )
    return out, cupom_grp

vendas, cupom_grp = _normalize_vendas_period(vend_raw)

# =========================
# Normalização COMPRAS (período)
# =========================
def _normalize_compras_period(c: pd.DataFrame) -> pd.DataFrame:
    if c.empty:
        return pd.DataFrame(columns=["Data_d","TotalNum"])
    c = c.copy()
    c.columns = [x.strip() for x in c.columns]
    col_data = _first_col(c, ["Data"])
    col_tot  = _first_col(c, ["Total","TotalLinha"])
    out = pd.DataFrame({
        "Data": c[col_data] if col_data else None,
        "TotalLinha": c[col_tot] if col_tot else 0
    })
    out["Data_d"]   = out["Data"].apply(_parse_date_any)
    out["TotalNum"] = out["TotalLinha"].apply(_to_float)
    out = out[(out["Data_d"]>=dt_ini) & (out["Data_d"]<=dt_fim)]
    return out

compras = _normalize_compras_period(comp_raw)

# =========================
# >>> Estoque & Custo Médio calculados da HISTÓRIA inteira <<<
# (independente da aba Produtos, sem botão de sincronização)
# =========================
def _normalize_vendas_all(v: pd.DataFrame) -> pd.DataFrame:
    if v.empty: 
        return pd.DataFrame(columns=["IDProduto","QtdNum"])
    v = v.copy()
    v.columns = [c.strip() for c in v.columns]
    col_idp = _first_col(v, ["IDProduto","ProdutoID","ID"])
    col_qtd = _first_col(v, ["Qtd","Quantidade","Qtde","Qde"])
    col_vid = _first_col(v, ["VendaID","Pedido","Cupom"])
    out = pd.DataFrame({
        "IDProduto": v[col_idp] if col_idp else None,
        "QtdNum": v[col_qtd].apply(_to_float) if col_qtd else 0.0,
        "VendaID": v[col_vid] if col_vid else ""
    })
    out["IDProduto"] = out["IDProduto"].astype(str)
    return out

def _normalize_compras_all(c: pd.DataFrame) -> pd.DataFrame:
    if c.empty:
        return pd.DataFrame(columns=["IDProduto","QtdNum","CustoNum"])
    c = c.copy()
    c.columns = [x.strip() for x in c.columns]
    col_idp = _first_col(c, ["IDProduto","ProdutoID","ID"])
    col_qtd = _first_col(c, ["Qtd","Quantidade","Qtde","Qde"])
    col_cu  = _first_col(c, ["Custo Unitário","CustoUnitário","CustoUnit","Custo Unit","Custo"])
    out = pd.DataFrame({
        "IDProduto": c[col_idp] if col_idp else None,
        "QtdNum": c[col_qtd].apply(_to_float) if col_qtd else 0.0,
        "CustoNum": c[col_cu].apply(_to_float) if col_cu else 0.0,
    })
    out["IDProduto"] = out["IDProduto"].astype(str)
    return out

def _normalize_ajustes_all(a: pd.DataFrame) -> pd.DataFrame:
    if a is None or a.empty:
        return pd.DataFrame(columns=["IDProduto","QtdNum"])
    a = a.copy()
    a.columns = [x.strip() for x in a.columns]
    col_idp = _first_col(a, ["IDProduto","ProdutoID","ID"])
    col_qtd = _first_col(a, ["Qtd","Quantidade","Qtde","Qde","Ajuste"])
    if not col_idp or not col_qtd:
        return pd.DataFrame(columns=["IDProduto","QtdNum"])
    out = pd.DataFrame({
        "IDProduto": a[col_idp].astype(str),
        "QtdNum": a[col_qtd].apply(_to_float),
    })
    return out

# Ajustes podem não existir
try: aj_raw = carregar_aba("Ajustes")
except Exception: aj_raw = pd.DataFrame()

v_all = _normalize_vendas_all(vend_raw)
c_all = _normalize_compras_all(comp_raw)
a_all = _normalize_ajustes_all(aj_raw)

entradas = c_all.groupby("IDProduto")["QtdNum"].sum() if not c_all.empty else pd.Series(dtype=float)
saidas   = v_all.groupby("IDProduto")["QtdNum"].sum() if not v_all.empty else pd.Series(dtype=float)
ajustes  = a_all.groupby("IDProduto")["QtdNum"].sum() if not a_all.empty else pd.Series(dtype=float)

calc = pd.DataFrame({"Entradas": entradas, "Saidas": saídas, "Ajustes": ajustes}).fillna(0.0)
calc["EstoqueCalc"] = calc["Entradas"] - calc["Saidas"] + calc["Ajustes"]

# custo médio (ponderado pelas compras)
if not c_all.empty:
    cm = c_all.assign(Parcial=c_all["QtdNum"]*c_all["CustoNum"]).groupby("IDProduto")[["Parcial","QtdNum"]].sum()
    cm["CustoMedio"] = cm["Parcial"] / cm["QtdNum"].replace(0, pd.NA)
    custo_medio = cm["CustoMedio"].fillna(0.0)
else:
    custo_medio = pd.Series(dtype=float)

calc["CustoMedio"] = custo_medio
calc = calc.reset_index().rename(columns={"index":"IDProduto"})

# Merge com PRODUTOS por ID (apenas para nomes/categoria/etc)
prod_calc = prod.copy() if not prod.empty else pd.DataFrame()
if not prod_calc.empty and "ID" in prod_calc.columns:
    prod_calc["ID"] = prod_calc["ID"].astype(str)
    prod_calc = prod_calc.merge(calc, how="left", left_on="ID", right_on="IDProduto")
    prod_calc.drop(columns=["IDProduto"], inplace=True)
else:
    # se não tem aba Produtos, mostramos só o cálculo
    prod_calc = calc.rename(columns={"IDProduto":"ID"})

for col in ["EstoqueCalc","CustoMedio","Entradas","Saidas","Ajustes"]:
    if col not in prod_calc.columns:
        prod_calc[col] = 0.0

prod_calc["ValorEstoqueCalc"] = prod_calc["CustoMedio"].fillna(0)*prod_calc["EstoqueCalc"].fillna(0)

# =========================
# KPIs (faturamento, cupons, itens, lucro, margem, ticket, caixa)
# =========================
if not vendas.empty:
    faturamento = cupom_grp["ReceitaCupom"].sum()
    num_cupons  = cupom_grp["VendaID"].nunique()
    itens_vendidos = vendas["QtdNum"].sum()
else:
    faturamento = 0.0; num_cupons = 0; itens_vendidos = 0.0

# COGS = sum(Qtd * CustoMedio[produto])
if not vendas.empty and not prod_calc.empty and "ID" in prod_calc.columns:
    custo_map = prod_calc.set_index("ID")["CustoMedio"].to_dict()
    idcol = "IDProduto" if "IDProduto" in vendas.columns else None
    if idcol:
        vendas["_CustoLinha"] = vendas["QtdNum"] * vendas[idcol].map(lambda x: _to_float(custo_map.get(str(x), 0)))
        cogs = vendas["_CustoLinha"].sum()
    else:
        cogs = 0.0
else:
    cogs = 0.0

lucro_bruto = max(0.0, faturamento - cogs)
margem_bruta = (lucro_bruto / faturamento * 100) if faturamento > 0 else 0.0
ticket_medio = (faturamento / num_cupons) if num_cupons > 0 else 0.0

# Caixa (mantido do seu base): Vendas - Compras
compras_total = compras["TotalNum"].sum() if not compras.empty else 0.0
caixa_periodo = faturamento - compras_total

# =========================
# KPIs (cards)
# =========================
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("💵 Faturamento (período)", _fmt_brl(faturamento))
k2.metric("🧾 Cupons", f"{num_cupons}", f"Ticket {_fmt_brl(ticket_medio)}")
k3.metric("📦 Itens vendidos", f"{itens_vendidos:.0f}")
k4.metric("📈 Lucro bruto (aprox.)", _fmt_brl(lucro_bruto), f"{margem_bruta:.1f}% margem")
k5.metric("🧮 Caixa (Vendas - Compras)", _fmt_brl(caixa_periodo))
st.caption(f"Período: {dt_ini.strftime('%d/%m/%Y')} a {dt_fim.strftime('%d/%m/%Y')}  •  Estornos {'INCLUÍDOS' if inclui_estornos else 'EXCLUÍDOS'}")

st.divider()

# =========================
# Vendas vs Compras por dia (robusto)
# =========================
st.subheader("📆 Vendas vs Compras por dia")

def _daily(df_in, date_col, val_col, label):
    if df_in is None or df_in.empty: 
        return pd.DataFrame(columns=["Data","Valor","Tipo"])
    d = df_in.copy()
    # evita nomes duplicados
    d = _unique_cols(d)
    # garante a coluna de data/valor
    d[date_col] = d[date_col].apply(_parse_date_any)
    out = d.groupby(date_col)[val_col].sum().reset_index().rename(columns={date_col:"Data", val_col:"Valor"})
    out["Tipo"] = label
    # ordena e garante colunas finais
    out = out[["Data","Valor","Tipo"]]
    return out

g_v = _daily(cupom_grp if not vendas.empty else pd.DataFrame(), "Data_d", "ReceitaCupom", "Vendas")
g_c = _daily(compras, "Data_d", "TotalNum", "Compras")

# garante mesmíssima ordem/nomes de colunas
for df_ in (g_v, g_c):
    for c in ["Data","Valor","Tipo"]:
        if c not in df_.columns:
            df_[c] = pd.NA
    df_ = df_[["Data","Valor","Tipo"]]

serie = pd.concat([g_v, g_c], ignore_index=True)

if not serie.empty:
    fig = px.bar(serie, x="Data", y="Valor", color="Tipo", barmode="group")
    fig.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados no período selecionado.")

st.divider()

# =========================
# Por forma de pagamento
# =========================
st.subheader("💳 Vendas por forma de pagamento")
if not (vendas.empty or cupom_grp.empty):
    fpg = cupom_grp.groupby("Forma", dropna=False)["ReceitaCupom"].sum().reset_index().sort_values("ReceitaCupom", ascending=False)
    c1,c2 = st.columns([1.1,1])
    with c1:
        fig_fp = px.pie(fpg, names="Forma", values="ReceitaCupom")
        st.plotly_chart(fig_fp, use_container_width=True)
    with c2:
        st.dataframe(fpg.rename(columns={"Forma":"Forma de pagamento","ReceitaCupom":"Total (R$)"}),
                     use_container_width=True, hide_index=True)
else:
    st.info("Sem vendas para detalhar por forma de pagamento.")

st.divider()

# =========================
# Top produtos por faturamento
# =========================
st.subheader("🏆 Top produtos por faturamento")
if not vendas.empty:
    key = "IDProduto" if "IDProduto" in vendas.columns else None
    if key:
        g = vendas.groupby(key)["TotalNum"].sum().reset_index().sort_values("TotalNum", ascending=False).head(10)
        if not prod.empty and "ID" in prod.columns and "Nome" in prod.columns:
            g = g.merge(prod[["ID","Nome"]], how="left", left_on=key, right_on="ID")
            g["Produto"] = g["Nome"].fillna(g[key].astype(str))
        else:
            g["Produto"] = g[key].astype(str)
        c1,c2 = st.columns([1.2,1])
        with c1:
            figt = px.bar(g, x="Produto", y="TotalNum")
            figt.update_layout(yaxis_title="R$", xaxis_title="")
            st.plotly_chart(figt, use_container_width=True)
        with c2:
            st.dataframe(g[["Produto","TotalNum"]].rename(columns={"TotalNum":"Total (R$)"}),
                         use_container_width=True, hide_index=True)
    else:
        st.info("Não encontrei coluna de ID nas vendas para rankear produtos.")
else:
    st.info("Sem vendas no período.")

st.divider()

# =========================
# ESTOQUE — visão geral (calculado ao vivo, sem botão)
# =========================
st.subheader("📦 Estoque — visão geral")

if prod_calc.empty:
    st.info("Sem produtos para exibir.")
else:
    m = pd.Series(True, index=prod_calc.index)
    if cat_sel and "Categoria" in prod_calc.columns:  m &= prod_calc["Categoria"].astype(str).isin(cat_sel)
    if forn_sel and "Fornecedor" in prod_calc.columns: m &= prod_calc["Fornecedor"].astype(str).isin(forn_sel)
    if apenas_ativos and "Ativo" in prod_calc.columns:
        prod_calc["Ativo"] = prod_calc["Ativo"].astype(str).str.lower()
        m &= (prod_calc["Ativo"]=="sim")
    if busca:
        s = busca.lower()
        m &= prod_calc.apply(lambda r: s in " ".join([str(x).lower() for x in r.values]), axis=1)

    dfv = prod_calc[m].copy()

    total_produtos = len(dfv)
    valor_estoque  = dfv["ValorEstoqueCalc"].sum()
    abaixo_min     = int((dfv["EstoqueCalc"].fillna(0) <= dfv["EstoqueMin"].fillna(0)).sum()) if "EstoqueMin" in dfv.columns else 0

    k1,k2,k3 = st.columns(3)
    k1.metric("Produtos exibidos", f"{total_produtos}")
    k2.metric("💰 Valor em estoque", _fmt_brl(valor_estoque))
    k3.metric("⚠️ Abaixo do mínimo", f"{abaixo_min}")

    st.markdown("**📋 Lista de produtos (filtrada)**")
    cols_show = [c for c in ["ID","Nome","Categoria","Fornecedor","CustoMedio","EstoqueCalc","EstoqueMin","ValorEstoqueCalc","Ativo"] if c in dfv.columns]
    st.dataframe(dfv[cols_show].rename(columns={
        "CustoMedio":"CustoAtual",
        "EstoqueCalc":"EstoqueAtual",
        "ValorEstoqueCalc":"ValorEstoque"
    }) if cols_show else dfv, use_container_width=True, hide_index=True)
