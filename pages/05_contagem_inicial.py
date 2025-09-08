# pages/03_contagem_inicial.py — Contagem de estoque (definir nível)
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Contagem de estoque", page_icon="📋", layout="wide")
st.title("📋 Contagem de estoque (definir nível)")

# ---------- helpers ----------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
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
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
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

# ---------- carrega dados ----------
ABA_PROD, ABA_COMP, ABA_VEND, ABA_AJ = "Produtos", "Compras", "Vendas", "Ajustes"

try:
    dfp = carregar_aba(ABA_PROD)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos."); st.stop()

try:
    dfc = carregar_aba(ABA_COMP)
except Exception:
    dfc = pd.DataFrame()

try:
    dfv = carregar_aba(ABA_VEND)
except Exception:
    dfv = pd.DataFrame()

try:
    dfa = carregar_aba(ABA_AJ)
except Exception:
    dfa = pd.DataFrame()

# chaves
col_id_prod = _first_col(dfp, ["ID","Codigo","Código","SKU"])
col_nome    = _first_col(dfp, ["Nome","Produto","Descrição"])
if not col_id_prod:
    st.error("Não encontrei a coluna de ID na aba Produtos."); st.stop()

# Compras
col_c_id = _first_col(dfc, ["IDProduto","ProdutoID","ID Prod","ID_Produto","ID"])
col_c_q  = _first_col(dfc, ["Qtd","Quantidade","Qtde","Qde"])

# Vendas
col_v_id = _first_col(dfv, ["IDProduto","ProdutoID","ID Prod","ID_Produto","ID"])
col_v_q  = _first_col(dfv, ["Qtd","Quantidade","Qtde","Qde"])

# Ajustes
col_a_id = _first_col(dfa, ["IDProduto","ID"])  # aceita B=ID ou IDProduto
col_a_q  = _first_col(dfa, ["Qtd","Quantidade","Qtde","Qde","Ajuste"])

# ---------- estoque calculado ----------
entr = pd.Series(dtype=float)
if not dfc.empty and col_c_id and col_c_q:
    cc = dfc[[col_c_id, col_c_q]].copy()
    cc[col_c_q] = cc[col_c_q].map(_to_num)
    entr = cc.groupby(col_c_id, dropna=True)[col_c_q].sum()

sai = pd.Series(dtype=float)
if not dfv.empty and col_v_id and col_v_q:
    vv = dfv[[col_v_id, col_v_q]].copy()
    vv[col_v_q] = vv[col_v_q].map(_to_num)
    sai = vv.groupby(col_v_id, dropna=True)[col_v_q].sum()

ajs = pd.Series(dtype=float)
if not dfa.empty and col_a_id and col_a_q:
    aa = dfa[[col_a_id, col_a_q]].copy()
    aa[col_a_q] = aa[col_a_q].map(_to_num)
    ajs = aa.groupby(col_a_id, dropna=True)[col_a_q].sum()

estoque_calc = (pd.DataFrame({"Entradas": entr, "Saidas": sai, "Ajustes": ajs})
                .fillna(0.0).eval("Entradas - Saidas + Ajustes"))

# ---------- UI ----------
dfp["_ID_join"] = dfp[col_id_prod].astype(str)
opts = dfp[[col_id_prod, col_nome]].astype(str).fillna("")
label = lambda r: f"{r[col_id_prod]} — {r[col_nome]}" if col_nome else r[col_id_prod]
lista = ["(selecione)"] + opts.apply(label, axis=1).tolist()

sel = st.selectbox("Produto", lista, index=0)
if sel != "(selecione)":
    pid = sel.split(" — ")[0]
    atual = float(estoque_calc.get(pid, 0.0))
    st.info(f"Estoque atual (calculado): **{atual:.0f}**")

    nova = st.number_input("Definir estoque para:", min_value=0, step=1, value=int(atual))
    delta = int(nova - atual)
    st.caption(f"Ajuste que será gravado: **{delta:+d}** (positivo entra / negativo sai)")

    if st.button("Salvar contagem"):
        if delta == 0:
            st.warning("Nada a ajustar — já está com essa quantidade.")
        else:
            sh = conectar_sheets()
            # garante/abre Ajustes
            try:
                ws = sh.worksheet(ABA_AJ)
            except Exception:
                ws = sh.add_worksheet(title=ABA_AJ, rows=1000, cols=6)
                ws.update("A1:F1", [["Data","ID","Qtd","Motivo","Responsável","Obs"]])

            # lê atual
            dfa2 = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            if dfa2.empty:
                dfa2 = pd.DataFrame(columns=["Data","ID","Qtd","Motivo","Responsável","Obs"])
            dfa2.columns = [c.strip() for c in dfa2.columns]

            # decide coluna de ID
            col_id_final = "ID" if "ID" in dfa2.columns else ("IDProduto" if "IDProduto" in dfa2.columns else "ID")

            nova_linha = {
                "Data": datetime.now().strftime("%d/%m/%Y"),
                col_id_final: pid,
                "Qtd": str(delta),
                "Motivo": "Contagem inicial" if atual == 0 else "Contagem",
                "Responsável": "",
                "Obs": ""
            }
            dfa2 = pd.concat([dfa2, pd.DataFrame([nova_linha])], ignore_index=True)

            ws.clear()
            set_with_dataframe(ws, dfa2)

            st.success(f"Contagem salva! Gravado ajuste de {delta:+d} para {pid}. Recarregue a página de estoque/produtos para ver o novo cálculo.")
else:
    st.info("Selecione um produto para definir o estoque.")
