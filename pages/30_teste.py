# -*- coding: utf-8 -*-
# pages/03_compras_entradas.py — Registrar compras/entradas de estoque + Telegram
import json, unicodedata, re
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

def _to_float(x):
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except: return ""

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

def _fmt_brl(v: float) -> str:
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

# ========= Telegram =========
def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return token, chat_id

def _tg_send(msg: str):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id: return
    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=6)
    except Exception:
        pass

# ========= headers e dados =========
PRODUTOS_ABA = "Produtos"
COMPRAS_ABA  = "Compras"
VENDAS_ABA   = "Vendas"
AJUSTES_ABA  = "Ajustes"
MOVS_ABA     = "MovimentosEstoque"

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
# compatível com outras páginas
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

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
    "id":   _pick_col(prod_df, ["ID","Id","id","Codigo","Código","SKU"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "forn": _pick_col(prod_df, ["Fornecedor","FornecedorNome"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid","Und"]),
}

# ------- cálculo de estoque (Compras - Vendas + Ajustes) -------
def _estoque_atual(pid: str="", nome: str="") -> float:
    pid = (pid or "").strip()
    nome = (nome or "").strip()

    def _sum(df, col_q, filtro):
        if df.empty or not col_q: return 0.0
        try:
            sub = df[filtro].copy()
            if sub.empty: return 0.0
            return pd.to_numeric(sub[col_q].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce").fillna(0).sum()
        except Exception:
            return 0.0

    try: comp = _load_df(COMPRAS_ABA)
    except Exception: comp = pd.DataFrame()
    try: vend = _load_df(VENDAS_ABA)
    except Exception: vend = pd.DataFrame()
    try: ajus = _load_df(AJUSTES_ABA)
    except Exception: ajus = pd.DataFrame()

    # Compras
    c_pid = _pick_col(comp, ["IDProduto","ProdutoID","ID"])
    c_qtd = _pick_col(comp, ["Qtd","Quantidade"])
    c_nom = _pick_col(comp, ["Produto","Nome"])
    ent = 0.0
    if c_qtd:
        if pid and c_pid:
            ent += _sum(comp, c_qtd, comp[c_pid].astype(str).str.strip()==pid)
        if nome and c_nom:
            ent += _sum(comp, c_qtd, (comp[c_nom].astype(str).str.strip()==nome) & (False if not c_pid else comp[c_pid].astype(str).str.strip().eq("").fillna(True)))

    # Vendas (qtd já considera estorno negativo)
    v_pid = _pick_col(vend, ["IDProduto","ProdutoID","ID"])
    v_qtd = _pick_col(vend, ["Qtd","Quantidade"])
    sai = 0.0
    if v_qtd:
        if pid and v_pid:
            sai += _sum(vend, v_qtd, vend[v_pid].astype(str).str.strip()==pid)
        if nome and _pick_col(vend, ["Produto","Nome"]):
            # normalmente não tem nome em Vendas; deixo só por segurança
            vn = _pick_col(vend, ["Produto","Nome"])
            sai += _sum(vend, v_qtd, vend[vn].astype(str).str.strip()==nome)

    # Ajustes
    a_pid = _pick_col(ajus, ["ID","IDProduto","ProdutoID"])
    a_qtd = _pick_col(ajus, ["Qtd","Quantidade","Qtde"])
    aj = 0.0
    if a_qtd and a_pid:
        if pid:
            aj += _sum(ajus, a_qtd, ajus[a_pid].astype(str).str.strip()==pid)
        elif nome:
            # se não tem ID no ajuste, tente por nome (se houver)
            an = _pick_col(ajus, ["Produto","Nome"])
            if an:
                aj += _sum(ajus, a_qtd, ajus[an].astype(str).str.strip()==nome)

    return float(ent - sai + aj)

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
            return n + (f" — " + f if f else "")

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
    salvar     = st.form_submit_button("➕ Registrar entrada", use_container_width=True)

if salvar:
    if not prod_nome.strip():
        st.error("Selecione ou digite um produto."); st.stop()
    qtd_f = _to_float(qtd); cst_f = _to_float(custo)
    if qtd_f in ("", None) or cst_f in ("", None):
        st.error("Preencha **Qtd** e **Custo unitário**."); st.stop()

    # estoque antes (antes de gravar)
    estoque_antes = _estoque_atual(pid=_nz(prod_id), nome=_nz(prod_nome))
    estoque_depois = estoque_antes + float(qtd_f)

    ws_compras = _ensure_ws(COMPRAS_ABA, COMPRAS_HEADERS)
    ws_mov     = _ensure_ws(MOVS_ABA,     MOV_HEADERS)

    total = round(float(qtd_f) * float(cst_f), 2)
    data_str = data_c.strftime("%d/%m/%Y")

    _append_row(ws_compras, {
        "Data": data_str,
        "Produto": _nz(prod_nome),
        "Unidade": _nz(unid),
        "Fornecedor": _nz(fornecedor),
        "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
        "Custo Unitário": f"{float(cst_f):.2f}".replace(".", ","),
        "Total": f"{total:.2f}".replace(".", ","),
        "IDProduto": _nz(prod_id),
        "Obs": _nz(obs)
    })
    _append_row(ws_mov, {
        "Data": data_str,
        "IDProduto": _nz(prod_id),
        "Produto": _nz(prod_nome),
        "Tipo": "B entrada",
        "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
        "Obs": ("Compra — " + _nz(obs)).strip(" —"),
        "ID": "",
        "Documento/NF": "",
        "Origem": "Compras / Entradas",
        "SaldoApós": str(int(estoque_depois)) if float(estoque_depois).is_integer() else str(estoque_depois).replace(".", ",")
    })

    # ---- Telegram ----
    msg = (
        "🧾 <b>Entrada de estoque registrada</b>\n"
        f"{data_str}\n"
        f"Produto: <b>{_nz(prod_nome)}</b>\n"
        f"Qtd: <b>{int(qtd_f) if float(qtd_f).is_integer() else qtd_f}</b> {_nz(unid) or 'un'}\n"
        f"Custo unit.: <b>{_fmt_brl(float(cst_f))}</b>\n"
        f"Total: <b>{_fmt_brl(total)}</b>\n"
        + (f"Fornecedor: {_nz(fornecedor)}\n" if _nz(fornecedor) else "")
        + (f"📦 Estoque: {int(estoque_antes)} → <b>{int(estoque_depois)}</b>\n" if isinstance(estoque_antes, (int,float)) else "")
        + (f"Obs.: {_nz(obs)}" if _nz(obs) else "")
    )
    _tg_send(msg)

    st.success("Entrada registrada com sucesso! ✅")
    st.toast("Compra lançada", icon="✅")

st.divider()
st.page_link("pages/02_cadastrar_produto.py", label="↩️ Voltar ao Cadastro/Editar", icon="➕")
st.page_link("pages/01_produtos.py", label="📦 Ir ao Catálogo", icon="📦")
