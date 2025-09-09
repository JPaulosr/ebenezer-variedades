# -*- coding: utf-8 -*-
# pages/01_fechamento_caixa.py — Fechamento de caixa (líquido por cupom + COGS)
import json, unicodedata, re
from collections.abc import Mapping
from datetime import datetime, date

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Fechamento de caixa", page_icon="🧾", layout="wide")
st.title("🧾 Fechamento de caixa")

# ========= Helpers =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    if not isinstance(svc, Mapping): st.error("🛑 GCP_SERVICE_ACCOUNT inválido."); st.stop()
    pk = str(svc.get("private_key",""))
    if "BEGIN PRIVATE KEY" not in pk: st.error("🛑 private_key inválida."); st.stop()
    return {**svc, "private_key": _normalize_private_key(pk)}

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc     = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL","")
    if not url_or_id: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=20)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ","").replace("\u00A0","")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]","", s)
    if s.count(".")>1:
        parts = s.split("."); s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

def _parse_date_any(s):
    if s is None or (isinstance(s, float) and pd.isna(s)): return None
    txt = str(s).strip()
    fmts = ("%d/%m/%Y","%Y-%m-%d","%d/%m/%y")
    for fmt in fmts:
        try: return datetime.strptime(txt, fmt).date()
        except: pass
    try: return pd.to_datetime(txt, dayfirst=True, errors="coerce").date()
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
    try: return ("R$ "+f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except: return "R$ 0,00"

# 🔑 ID canônico: remove tudo que não for dígito
def _canon_id(x: object) -> str:
    return re.sub(r"[^0-9]", "", str(x or ""))

# ========= Carregar bases =========
ABA_PROD, ABA_VEND, ABA_COMP = "Produtos", "Vendas", "Compras"
try: prod = carregar_aba(ABA_PROD)
except: prod = pd.DataFrame()
try: vend_raw = carregar_aba(ABA_VEND)
except: vend_raw = pd.DataFrame()
try: comp_raw = carregar_aba(ABA_COMP)
except: comp_raw = pd.DataFrame()

# ========= Filtros de período =========
c1, c2 = st.columns(2)
with c1: de = st.date_input("De", value=date.today())
with c2: ate = st.date_input("Até", value=date.today())
inclui_estornos = st.checkbox("Incluir estornos (CN-/ESTORNO)", value=False)

# ========= Normalizar VENDAS (período) =========
def _normalize_vendas_period(v: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if v.empty: return pd.DataFrame(), pd.DataFrame()
    v = v.copy(); v.columns = [c.strip() for c in v.columns]
    col_data  = _first_col(v, ["Data"])
    col_vid   = _first_col(v, ["VendaID","Pedido","Cupom"])
    col_idp   = _first_col(v, ["IDProduto","ProdutoID","ID"])
    col_qtd   = _first_col(v, ["Qtd","Quantidade","Qtde","Qde"])
    col_pu    = _first_col(v, ["PrecoUnit","Preço Unitário","PreçoUnitário","Preço","Preco"])
    col_tot   = _first_col(v, ["TotalLinha","Total"])
    col_forma = _first_col(v, ["FormaPagto","Forma Pagamento","FormaPagamento","Pagamento","Forma"])
    col_desc  = _first_col(v, ["Desconto"])
    col_totcup= _first_col(v, ["TotalCupom"])
    col_stat  = _first_col(v, ["CupomStatus","Status"])

    out = pd.DataFrame({
        "Data": v[col_data] if col_data else None,
        "VendaID": v[col_vid] if col_vid else "",
        "IDProduto": v[col_idp] if col_idp else None,
        "Qtd": v[col_qtd] if col_qtd else 0,
        "PrecoUnit": v[col_pu] if col_pu else 0,
        "TotalLinha": v[col_tot] if col_tot else 0,
        "Forma": v[col_forma] if col_forma else "",
        "Desconto": v[col_desc] if col_desc else 0,
        "TotalCupom": v[col_totcup] if col_totcup else None,
        "CupomStatus": v[col_stat] if col_stat else None
    })

    out["Data_d"]   = out["Data"].apply(_parse_date_any)
    out             = out[(out["Data_d"]>=de) & (out["Data_d"]<=ate)]
    out["QtdNum"]   = out["Qtd"].apply(_to_float)
    out["PrecoNum"] = out["PrecoUnit"].apply(_to_float)
    out["TotalNum"] = out["TotalLinha"].apply(_to_float)   # BRUTO por linha
    out["DescNum"]  = out["Desconto"].apply(_to_float)
    out["CupomNum"] = out["TotalCupom"].apply(_to_float)
    out["VendaID"]  = out["VendaID"].astype(str).fillna("")
    out["IDProduto"]= out["IDProduto"].astype(str)
    out["KeyID"]    = out["IDProduto"].apply(_canon_id)
    out["is_estorno"] = out["VendaID"].str.startswith("CN-") | (out["CupomStatus"].astype(str).str.upper()=="ESTORNO")
    if not inclui_estornos: out = out[~out["is_estorno"]]

    # Receita líquida por cupom
    cupom = out.groupby("VendaID", dropna=True).agg({
        "Data_d":"first", "Forma":"first",
        "TotalNum":"sum", "DescNum":"max", "CupomNum":"max"
    }).reset_index()
    cupom["ReceitaCupom"] = cupom.apply(
        lambda r: r["CupomNum"] if r["CupomNum"]>0 else max(0.0, r["TotalNum"] - r["DescNum"]),
        axis=1
    )
    return out, cupom

vendas, cupom = _normalize_vendas_period(vend_raw)

# ========= Custo médio (map) por KeyID =========
def _custo_medio_map(comp_df: pd.DataFrame, prod_df: pd.DataFrame) -> dict:
    mp = {}
    # 1) custo médio das compras
    if not comp_df.empty:
        c = comp_df.copy(); c.columns = [x.strip() for x in c.columns]
        col_idp = _first_col(c, ["IDProduto","ProdutoID","ID"])
        col_qtd = _first_col(c, ["Qtd","Quantidade","Qtde","Qde"])
        col_cu  = _first_col(c, ["Custo Unitário","CustoUnitário","CustoUnit","Custo Unit","Custo"])
        if col_idp and col_qtd and col_cu:
            c["KeyID"]    = c[col_idp].apply(_canon_id)
            c["QtdNum"]   = c[col_qtd].apply(_to_float)
            c["CustoNum"] = c[col_cu].apply(_to_float)
            c = c[c["KeyID"]!=""]
            c["Parcial"]  = c["QtdNum"]*c["CustoNum"]
            g = c.groupby("KeyID")[["Parcial","QtdNum"]].sum()
            g["CustoMedio"] = g["Parcial"] / g["QtdNum"].replace(0, pd.NA)
            mp.update(g["CustoMedio"].fillna(0.0).to_dict())
    # 2) fallback: Produtos.CustoAtual
    if not prod_df.empty:
        pid = _first_col(prod_df, ["ID","Id","id"])
        if pid and "CustoAtual" in prod_df.columns:
            aux = prod_df[[pid,"CustoAtual"]].copy()
            aux["KeyID"] = aux[pid].apply(_canon_id)
            for _, r in aux.iterrows():
                if r["KeyID"] and (r["KeyID"] not in mp or mp[r["KeyID"]]==0):
                    mp[r["KeyID"]] = float(_to_float(r["CustoAtual"]))
    return mp

custo_map = _custo_medio_map(comp_raw, prod)

# ========= KPIs / Bruto x Líquido x COGS =========
if vendas.empty:
    cupons = 0; itens = 0; receita = 0.0; bruto = 0.0; desc_tot = 0.0
else:
    cupons  = cupom["VendaID"].nunique()
    itens   = vendas["QtdNum"].sum()
    bruto   = cupom["TotalNum"].sum()          # BRUTO (soma TotalLinha)
    receita = cupom["ReceitaCupom"].sum()      # LÍQUIDO
    desc_tot= max(0.0, bruto - receita)

# COGS do período por KeyID
if vendas.empty or not custo_map:
    cogs = 0.0
else:
    v = vendas.copy()
    v = v[v["KeyID"]!=""]
    cogs = float((v["QtdNum"] * v["KeyID"].map(lambda k: float(custo_map.get(k, 0.0)))).sum())

lucro  = max(0.0, receita - cogs)
margem = (lucro/receita*100) if receita>0 else 0.0

k1,k2,k3,k4 = st.columns(4)
k1.metric("Cupons (vendas)", f"{cupons}")
k2.metric("Itens vendidos", f"{itens:.0f}")
k3.metric("Faturamento líquido", _fmt_brl(receita), f"Bruto {_fmt_brl(bruto)}")
k4.metric("Lucro bruto (estimado)", _fmt_brl(lucro), f"{margem:.1f}% margem")
st.caption(f"Período: {de.strftime('%d/%m/%Y')} a {ate.strftime('%d/%m/%Y')} • Estornos {'INCLUÍDOS' if inclui_estornos else 'EXCLUÍDOS'} • Descontos aplicados: {_fmt_brl(desc_tot)}")

st.divider()

# ========= Por forma de pagamento (líquido) =========
st.subheader("Por forma de pagamento")
if not cupom.empty:
    fpg = cupom.groupby("Forma", dropna=False)["ReceitaCupom"].sum().reset_index().sort_values("ReceitaCupom", ascending=False)
    fig = px.bar(fpg, x="Forma", y="ReceitaCupom")
    fig.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(fpg.rename(columns={"Forma":"Forma de pagamento","ReceitaCupom":"Total (R$)"}),
                 use_container_width=True, hide_index=True)
else:
    st.info("Sem vendas no período.")

st.divider()

# ========= Resumo por produto (alocando desconto proporcional) =========
st.subheader("Resumo por produto (período)")
if vendas.empty:
    st.info("Sem vendas para detalhar.")
else:
    tmp = vendas.merge(cupom[["VendaID","ReceitaCupom","TotalNum"]].rename(columns={"TotalNum":"BrutoCupom"}),
                       how="left", on="VendaID")
    tmp["ReceitaLinha"] = tmp.apply(
        lambda r: r["TotalNum"] * (r["ReceitaCupom"]/r["BrutoCupom"]) if r.get("BrutoCupom",0)>0 else r["TotalNum"],
        axis=1
    )
    base = tmp[["KeyID","IDProduto","QtdNum","ReceitaLinha","TotalNum"]].copy()
    grp = (
        base.groupby("KeyID", dropna=False)
            .agg(Qtd=("QtdNum","sum"),
                 Receita=("ReceitaLinha","sum"),
                 ReceitaBruta=("TotalNum","sum"))
            .reset_index()
    )
    grp["COGS"]  = grp.apply(lambda r: r["Qtd"] * float(custo_map.get(str(r["KeyID"]), 0.0)), axis=1)
    grp["Lucro"] = grp["Receita"] - grp["COGS"]

    # juntar nome a partir de Produtos
    if not prod.empty:
        pid = _first_col(prod, ["ID","Id","id"])
        nm  = _first_col(prod, ["Nome","Produto","Descricao","Descrição"])
        aux = prod[[pid, nm]].rename(columns={pid:"ID", nm:"Nome"})
        aux["KeyID"] = aux["ID"].apply(_canon_id)
        grp = grp.merge(aux[["KeyID","Nome"]], how="left", on="KeyID")
        grp["Produto"] = grp["Nome"].fillna(grp["KeyID"])
    else:
        grp["Produto"] = grp["KeyID"]

    grp = grp[["Produto","Qtd","Receita","COGS","Lucro","ReceitaBruta"]].sort_values("Receita", ascending=False)
    st.dataframe(grp, use_container_width=True, hide_index=True)
    st.download_button(
        "⬇️ Exportar (CSV)",
        grp.to_csv(index=False).encode("utf-8"),
        file_name=f"fechamento_{de:%Y%m%d}_{ate:%Y%m%d}.csv",
        mime="text/csv"
    )
