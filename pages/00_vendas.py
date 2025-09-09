# -*- coding: utf-8 -*-
# pages/05_vendas.py — Registrar Vendas (baixa automática no estoque)
import json, unicodedata, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date, datetime

st.set_page_config(page_title="Vendas", page_icon="🛒", layout="wide")
st.title("🛒 Vendas")

# ========= credenciais (mesmo padrão das outras páginas) =========
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
    if isinstance(svc, str):
        svc = json.loads(svc)
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
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# ========= utilidades =========
def _to_float_or_zero(x):
    """Converte '1,16', '11.60', '1.234,56' etc. em float seguro."""
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
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

def _pick_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

# ========= Abas & headers =========
ABA_PRODUTOS  = "Produtos"
ABA_VENDAS    = "Vendas"
ABA_MOV       = "MovimentosEstoque"

VENDAS_HEADERS = [
    "Data","Produto","IDProduto","Qtd","Preço Unitário","Total",
    "Forma Pagamento","Obs"
]
MOV_HEADERS = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

# ========= Carrega dados =========
try:
    prod_df = _load_df(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"):
        st.code(str(e))
    st.stop()

COLP = {
    "id":   _pick_col(prod_df, ["ID","Id","id"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid"]),
    "pv":   _pick_col(prod_df, ["Preço","Preco","Preço Venda","PrecoVenda","PreçoUnit","PrecoUnit","PV","Venda"])
}
for k,v in COLP.items():
    if v is None and k in ("id","nome"):
        prod_df[k.upper()] = ""
        COLP[k] = k.upper()

# tenta carregar vendas existentes (pode não existir ainda)
try:
    vendas_df = _load_df(ABA_VENDAS)
except Exception:
    vendas_df = pd.DataFrame(columns=VENDAS_HEADERS)

# ========= Formulário de venda =========
st.subheader("Registrar nova venda")
with st.form("form_venda"):
    usar_lista = st.checkbox("Selecionar produto da lista", value=True)
    if usar_lista:
        if prod_df.empty:
            st.warning("Sem produtos cadastrados."); st.stop()

        def _fmt(r):
            n = _nz(r.get(COLP["nome"], "")) or "(sem nome)"
            pid = _nz(r.get(COLP["id"], ""))
            pv_sug = _to_float_or_zero(r.get(COLP["pv"], ""))
            sugerido = f" • R$ {pv_sug:.2f}" if pv_sug > 0 else ""
            return f"{n} ({pid}){sugerido}"

        labels = prod_df.apply(_fmt, axis=1).tolist()
        idx = st.selectbox("Produto", options=range(len(prod_df)), format_func=lambda i: labels[i])
        row = prod_df.iloc[idx]
        prod_nome = _nz(row.get(COLP["nome"], ""))
        prod_id   = _nz(row.get(COLP["id"], ""))
        preco_sug = _to_float_or_zero(row.get(COLP["pv"], ""))
    else:
        prod_nome = st.text_input("Produto (nome exato)")
        prod_id   = st.text_input("ID (opcional)")
        preco_sug = 0.0

    c1, c2, c3 = st.columns([1,1,1])
    with c1: data_v = st.date_input("Data da venda", value=date.today())
    with c2: qtd    = st.text_input("Qtd", placeholder="Ex.: 2")
    with c3: preco  = st.text_input("Preço unitário (R$)", value=(f"{preco_sug:.2f}" if preco_sug>0 else ""), placeholder="Ex.: 19,90")

    formas_existentes = sorted(set(vendas_df.get("Forma Pagamento", pd.Series(dtype=str)).dropna().astype(str).str.strip()))
    if not formas_existentes:
        formas_existentes = ["Dinheiro","Cartão","Pix","Fiado"]
    forma_pg = st.selectbox("Forma de pagamento", options=formas_existentes)
    obs      = st.text_input("Observações (opcional)")

    salvar = st.form_submit_button("💾 Registrar venda")

if salvar:
    if not prod_nome.strip():
        st.error("Selecione ou digite um produto.")
        st.stop()
    q = _to_float_or_zero(qtd)
    p = _to_float_or_zero(preco)
    if q <= 0 or p <= 0:
        st.error("Informe **Qtd** e **Preço unitário** válidos (> 0).")
        st.stop()

    total = round(q * p, 2)

    ws_vendas = _ensure_ws(ABA_VENDAS, VENDAS_HEADERS)
    ws_mov    = _ensure_ws(ABA_MOV,    MOV_HEADERS)

    # 1) registra a venda
    _append_row(ws_vendas, {
        "Data": data_v.strftime("%d/%m/%Y"),
        "Produto": prod_nome,
        "IDProduto": prod_id,
        "Qtd": (str(int(q)) if float(q).is_integer() else str(q)).replace(".", ","),
        "Preço Unitário": f"{p:.2f}",
        "Total": f"{total:.2f}",
        "Forma Pagamento": _nz(forma_pg),
        "Obs": _nz(obs)
    })

    # 2) baixa no estoque (MovimentosEstoque)
    _append_row(ws_mov, {
        "Data": data_v.strftime("%d/%m/%Y"),
        "IDProduto": prod_id,
        "Produto": prod_nome,
        "Tipo": "saida",
        "Qtd": (str(int(q)) if float(q).is_integer() else str(q)).replace(".", ","),
        "Obs": f"Venda — {_nz(obs)}".strip()
    })

    st.success("Venda registrada e estoque baixado com sucesso! ✅")
    st.toast("Venda lançada", icon="🛒")

st.divider()

# ========= Resumo do dia =========
st.subheader("Resumo do dia")
hoje_str1 = date.today().strftime("%d/%m/%Y")   # formato da planilha
hoje_str2 = date.today().isoformat()            # caso alguém use ISO

# Recarrega vendas para resumo
try:
    vendas_df = _load_df(ABA_VENDAS)
except Exception:
    vendas_df = pd.DataFrame(columns=VENDAS_HEADERS)

if not vendas_df.empty and "Data" in vendas_df.columns:
    vd = vendas_df.copy()
    # filtra por hoje (aceita "dd/mm/aaaa" ou "aaaa-mm-dd")
    vd_today = vd[(vd["Data"].astype(str).str.strip() == hoje_str1) | (vd["Data"].astype(str).str.startswith(hoje_str2))]
    if vd_today.empty:
        st.info("Sem vendas registradas hoje.")
    else:
        vd_today["Qtd_num"]   = vd_today["Qtd"].apply(_to_float_or_zero)
        vd_today["Preco_num"] = vd_today["Preço Unitário"].apply(_to_float_or_zero)
        vd_today["Total_num"] = vd_today["Total"].apply(_to_float_or_zero)

        c1, c2, c3 = st.columns(3)
        with c1: st.metric("💵 Faturamento (hoje)", f"R$ {vd_today['Total_num'].sum():.2f}")
        with c2: st.metric("🧮 Itens vendidos", f"{vd_today['Qtd_num'].sum():.0f}")
        with c3: 
            por_fp = vd_today.groupby(vd_today["Forma Pagamento"].astype(str))["Total_num"].sum().sort_values(ascending=False)
            st.metric("💳 Principal forma pgto", por_fp.index[0] if not por_fp.empty else "-")

        st.markdown("**Vendas de hoje**")
        cols_show = [c for c in ["Data","Produto","IDProduto","Qtd","Preço Unitário","Total","Forma Pagamento","Obs"] if c in vd_today.columns]
        st.dataframe(vd_today[cols_show], use_container_width=True, hide_index=True)

        with st.expander("Por forma de pagamento"):
            if not por_fp.empty:
                st.dataframe(por_fp.reset_index().rename(columns={"Forma Pagamento":"Forma","Total_num":"Total R$"}),
                             use_container_width=True, hide_index=True)
else:
    st.info("Ainda não há vendas registradas.")

st.divider()
st.page_link("pages/01_produtos.py",           label="📦 Catálogo",                   icon="📦")
st.page_link("pages/03_compras_entradas.py",   label="🧾 Registrar Compras",          icon="🧾")
st.page_link("pages/04_estoque.py",            label="📦 Estoque — Movimentos",       icon="📦")
