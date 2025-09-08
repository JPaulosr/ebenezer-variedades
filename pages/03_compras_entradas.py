# -*- coding: utf-8 -*-
# pages/03_compras_entradas.py — Registrar compras/entradas de estoque
import json, unicodedata
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, date

st.set_page_config(page_title="Compras / Entradas", page_icon="🧾", layout="wide")
st.title("🧾 Compras / Entradas de Estoque")

# ========= credenciais =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
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

@st.cache_data
def _load_df(aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")  # <<< evita 'nan'

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
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

def _to_float(x):
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("R$", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except: return ""

def _nz(x):
    """Normaliza vazio/NaN para string vazia."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

# ========= headers e dados =========
PRODUTOS_ABA = "Produtos"
COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

try:
    prod_df = _load_df(PRODUTOS_ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"):
        st.code(str(e))
    st.stop()

def _pick_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

COL = {
    "id":   _pick_col(prod_df, ["ID","Id","id"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "forn": _pick_col(prod_df, ["Fornecedor","FornecedorNome"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid"]),
}

# ========= formulário =========
st.subheader("Nova compra / entrada")
with st.form("form_compra"):
    usar_lista = st.checkbox("Selecionar produto da lista", value=True)
    if usar_lista:
        if prod_df.empty:
            st.warning("Sem produtos cadastrados."); st.stop()

        def _fmt(r):
            n = _nz(r.get(COL["nome"], "")) or "(sem nome)"
            f = _nz(r.get(COL["forn"], ""))
            return n + (f" — {f}" if f else "")

        labels = prod_df.apply(_fmt, axis=1).tolist()
        idx = st.selectbox("Produto", options=range(len(prod_df)), format_func=lambda i: labels[i])
        row = prod_df.iloc[idx]
        prod_nome = _nz(row.get(COL["nome"], ""))
        prod_id   = _nz(row.get(COL["id"], ""))
        unid_sug  = _nz(row.get(COL["unid"], ""))
        forn_sug  = _nz(row.get(COL["forn"], ""))
    else:
        prod_nome = st.text_input("Produto (nome exato)")
        prod_id   = st.text_input("ID (opcional)")
        unid_sug  = ""
        forn_sug  = ""

    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1: data_c = st.date_input("Data da compra", value=date.today())
    with c2: qtd    = st.text_input("Qtd", placeholder="Ex.: 10")
    with c3: custo  = st.text_input("Custo unitário (R$)", placeholder="Ex.: 12,50")
    with c4: unid   = st.text_input("Unidade", value=unid_sug or "un")

    fornecedor = st.text_input("Fornecedor", value=forn_sug)
    obs        = st.text_input("Observações (opcional)")
    salvar     = st.form_submit_button("➕ Registrar entrada")

if salvar:
    if not prod_nome.strip():
        st.error("Selecione ou digite um produto.")
        st.stop()
    qtd_f = _to_float(qtd); cst_f = _to_float(custo)
    if qtd_f in ("", None, 0) or cst_f in ("", None, 0):
        st.error("Preencha **Qtd** e **Custo unitário**.")
        st.stop()

    ws_compras = _ensure_ws("Compras", COMPRAS_HEADERS)
    ws_mov     = _ensure_ws("MovimentosEstoque", MOV_HEADERS)

    total = round(float(qtd_f) * float(cst_f), 2)
    _append_row(ws_compras, {
        "Data": data_c.strftime("%d/%m/%Y"),
        "Produto": prod_nome,
        "Unidade": _nz(unid),
        "Fornecedor": _nz(fornecedor),
        "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
        "Custo Unitário": f"{float(cst_f):.2f}".replace(".", ","),
        "Total": f"{total:.2f}".replace(".", ","),
        "IDProduto": _nz(prod_id),
        "Obs": _nz(obs)
    })
    _append_row(ws_mov, {
        "Data": data_c.strftime("%d/%m/%Y"),
        "IDProduto": _nz(prod_id),
        "Produto": prod_nome,
        "Tipo": "entrada",
        "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
        "Obs": f"Compra — {_nz(obs)}".strip()
    })

    st.success("Entrada registrado com sucesso! ✅")
    st.toast("Compra lançada", icon="✅")

st.divider()
st.page_link("pages/02_cadastrar_produto.py", label="↩️ Voltar ao Cadastro/Editar", icon="➕")
st.page_link("pages/01_produtos.py", label="📦 Ir ao Catálogo", icon="📦")
