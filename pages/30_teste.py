# -*- coding: utf-8 -*-
# pages/04_estoque.py — Movimentos & Ajustes de Estoque (com Vendas)

import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date

st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📦", layout="wide")
st.title("📦 Estoque — Movimentos & Ajustes")

# ---------- refresh automático (limpa cache 1x ao abrir a página) ----------
if st.session_state.pop("_first_load_estoque", True):
    st.cache_data.clear()
st.session_state.setdefault("_first_load_estoque", False)

# ========= credenciais =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

# ↓ TTL=1s: sempre que você entrar na página, ele refaz leitura “fresca”
@st.cache_data(ttl=1, show_spinner=False)
def _load_df(aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        if cur.empty or any(h not in cur.columns for h in headers):
            cols = list(dict.fromkeys(headers + cur.columns.tolist()))
            df_head = pd.DataFrame(columns=cols)
            ws.clear()
            set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        df_head = pd.DataFrame(columns=headers)
        set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws

def _append_row(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0).fillna("")
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# ========= utilidades =========
def _to_float_or_zero(x):
    if x is None: return 0.0
    s = str(x).strip()
    if s == "": return 0.0
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except:
        return 0.0

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower()

def _norm_tipo(t: str) -> str:
    t = _strip_accents_low(t)
    t = re.sub(r"[^a-z]", "", t)
    if "entrada" in t or "compra" in t or "estorno" in t:
        return "entrada"
    if "saida" in t or "venda" in t or "baixa" in t:
        return "saida"
    if "ajuste" in t:
        return "ajuste"
    return "outro"

def _prod_key(pid, nome):
    return f"{_nz(pid)}||{_nz(nome)}".strip("|")

# ========= Abas =========
ABA_PRODUTOS  = "Produtos"
ABA_COMPRAS   = "Compras"
ABA_MOV       = "MovimentosEstoque"
ABA_VENDAS    = "Vendas"         # <— NOVO: vamos ler as saídas daqui

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# ======== Carregar bases ========
prod_df    = _load_df(ABA_PRODUTOS)
compras_df = _load_df(ABA_COMPRAS) if "Compras" in [ws.title for ws in _sheet().worksheets()] else pd.DataFrame(columns=COMPRAS_HEADERS)
mov_df     = _load_df(ABA_MOV)     if "MovimentosEstoque" in [ws.title for ws in _sheet().worksheets()] else pd.DataFrame(columns=MOV_HEADERS)
vendas_df  = _load_df(ABA_VENDAS)  if "Vendas" in [ws.title for ws in _sheet().worksheets()] else pd.DataFrame(columns=["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"])

# ======== Normalizações ========
# Produtos
COLP = {
    "id":   next((c for c in ["ID","Id","id","Codigo","Código","SKU"] if c in prod_df.columns), None),
    "nome": next((c for c in ["Nome","Produto","Descrição","Descricao"] if c in prod_df.columns), None),
}
if COLP["nome"] is None: st.error("Aba Produtos precisa ter coluna de nome (Nome/Produto/Descrição)."); st.stop()
prod_df["__key"] = prod_df.apply(lambda r: _prod_key(r.get(COLP["id"], ""), r.get(COLP["nome"], "")), axis=1)
prod_df["Produto"]   = prod_df[COLP["nome"]]
prod_df["IDProduto"] = prod_df[COLP["id"]] if COLP["id"] else ""

# Compras → custo atual (último)
for c in COMPRAS_HEADERS:
    if c not in compras_df.columns: compras_df[c] = ""
compras_df["__key"]      = compras_df.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)
compras_df["Qtd_num"]    = compras_df["Qtd"].apply(_to_float_or_zero)
compras_df["Custo_num"]  = compras_df["Custo Unitário"].apply(_to_float_or_zero)
if not compras_df.empty:
    last_cost = compras_df.groupby("__key", as_index=False).tail(1)
    custo_atual_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else:
    custo_atual_map = {}

# Movimentos → entradas/saídas/ajustes manuais
for c in MOV_HEADERS:
    if c not in mov_df.columns: mov_df[c] = ""
mov_df["Tipo_norm"]  = mov_df["Tipo"].apply(_norm_tipo)
mov_df["Qtd_num"]    = mov_df["Qtd"].apply(_to_float_or_zero)
mov_df["__key"]      = mov_df.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)

def _sum_mov(tipo):
    m = mov_df[mov_df["Tipo_norm"] == tipo]
    if m.empty: return {}
    return m.groupby("__key")["Qtd_num"].sum().to_dict()

entradas_mov = _sum_mov("entrada")
saidas_mov   = _sum_mov("saida")
ajustes_mov  = _sum_mov("ajuste")

# Vendas → saídas por produto (somente CupomStatus OK)
if not vendas_df.empty:
    vendas_df["Qtd_num"] = vendas_df["Qtd"].apply(_to_float_or_zero)
    vendas_df["__key"]   = vendas_df.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    vendas_ok = vendas_df[(vendas_df["__key"] != "") & (vendas_df.get("CupomStatus","OK").astype(str).str.upper() == "OK")]
    saidas_vendas = vendas_ok.groupby("__key")["Qtd_num"].sum().to_dict()
else:
    saidas_vendas = {}

# ======== Consolidação Estoque ========
df = prod_df[["__key","Produto","IDProduto"]].copy()

def _get(mapper, key): return float(mapper.get(key, 0.0))

df["Entradas"] = df["__key"].apply(lambda k: _get(entradas_mov, k) + compras_df.loc[compras_df["__key"]==k, "Qtd_num"].sum())
df["Saidas"]   = df["__key"].apply(lambda k: _get(saidas_mov, k) + _get(saidas_vendas, k))
df["Ajustes"]  = df["__key"].apply(lambda k: _get(ajustes_mov, k))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]

df["CustoAtual"] = df["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df["ValorTotal"] = (df["EstoqueAtual"].astype(float) * df["CustoAtual"].astype(float)).round(2)

# ======== RESUMO + Tabela ========
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("🧮 Itens com estoque > 0", int((df["EstoqueAtual"] > 0).sum()))
with c2:
    st.metric("📦 Quantidade total em estoque", f"{df['EstoqueAtual'].sum():.0f}")
with c3:
    st.metric("💰 Valor total (R$)", f"R$ {df['ValorTotal'].sum():.2f}")

st.subheader("Tabela de Estoque (Compras + Vendas + Ajustes)")
cols_show = ["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
st.dataframe(df[cols_show].sort_values("Produto"), use_container_width=True, hide_index=True)

# ======== Forms (Saída/Ajuste) permanecem iguais… ========
# (se quiser, mantenha seus blocos de registrar saída/ajuste como estavam)
