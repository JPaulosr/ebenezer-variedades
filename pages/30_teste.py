# pages/00_vendas.py — Vendas rápidas (carrinho + histórico/estorno/duplicar)
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date, timedelta
import re
import unicodedata as _ud

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # Telegram

st.set_page_config(page_title="Vendas rápidas", page_icon="🧾", layout="wide")
st.title("🧾 Vendas rápidas (carrinho)")

# ================= Helpers =================
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

@st.cache_data(ttl=10)
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

def _fmt_brl_num(v):
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _gerar_id(prefixo="F"):
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _garantir_aba(sh, nome, cols):
    try:
        ws = sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10,len(cols)))
        ws.update("A1", [cols])
        return ws
    headers = ws.row_values(1) or []
    headers = [h.strip() for h in headers]
    falt = [c for c in cols if c not in headers]
    if falt:
        ws.update("A1", [headers + falt])
    return ws

def _append_rows(ws, rows: list[dict]):
    headers = ws.row_values(1)
    hdr = [h.strip() for h in headers]
    to_append = []
    for d in rows:
        to_append.append([d.get(h, "") for h in hdr])
    if to_append:
        ws.append_rows(to_append, value_input_option="USER_ENTERED")

# -------- Telegram --------
def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return token, chat_id

def _tg_send(msg: str, photo_url: str | None = None):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id: return
    try:
        if photo_url:
            caption = msg if len(msg) <= 1000 else "🧾 Venda registrada — detalhes abaixo ⤵️"
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            payload = {
                "chat_id": str(chat_id),
                "photo": photo_url,
                "caption": caption,
                "parse_mode": "HTML",
                "disable_notification": True,
            }
            requests.post(url, json=payload, timeout=8)
            if caption != msg:
                url2 = f"https://api.telegram.org/bot{token}/sendMessage"
                payload2 = {
                    "chat_id": str(chat_id),
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                    "disable_notification": True,
                }
                requests.post(url2, json=payload2, timeout=8)
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": str(chat_id),"text": msg,"parse_mode": "HTML","disable_web_page_preview": True}
            requests.post(url, json=payload, timeout=8)
    except Exception:
        pass

def _tg_send_media_group(media: list[dict]):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id or not media: return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
        payload = {"chat_id": str(chat_id), "media": media}
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

# ---------------- Clientes ----------------
ABA_CLIENTES = "Clientes"
COLS_CLIENTES = ["Cliente","Telefone","Obs"]

def _strip_accents(s: str) -> str:
    if not isinstance(s, str): return ""
    return "".join(ch for ch in _ud.normalize("NFD", s) if _ud.category(ch) != "Mn")

def _normalize_cliente(nome: str) -> str:
    nome = (nome or "").strip()
    nome = re.sub(r"\s+", " ", nome)
    return nome.title()

def _cliente_key(nome: str) -> str:
    base = _normalize_cliente(nome)
    base = _strip_accents(base).lower()
    return re.sub(r"\s+", " ", base).strip()

def _carregar_clientes() -> list[str]:
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        if dfc.empty: return []
        col_cli = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        vistos = {}
        for raw in dfc[col_cli].dropna().astype(str):
            norm = _normalize_cliente(raw)
            k = _cliente_key(norm)
            if k and k not in vistos:
                vistos[k] = norm
        return sorted(vistos.values())
    except Exception:
        return []

def _ensure_cliente(cli_nome: str):
    cli_nome = _normalize_cliente(cli_nome)
    if not cli_nome:
        return
    sh = conectar_sheets()
    ws_cli = _garantir_aba(sh, ABA_CLIENTES, COLS_CLIENTES)
    try:
        dfc = carregar_aba(ABA_CLIENTES)
    except Exception:
        dfc = pd.DataFrame(columns=COLS_CLIENTES)
    ja_tem = False
    if not dfc.empty:
        col_cli = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        for raw in dfc[col_cli].dropna().astype(str):
            if _cliente_key(raw) == _cliente_key(cli_nome):
                ja_tem = True
                break
    if not ja_tem:
        _append_rows(ws_cli, [{"Cliente": cli_nome, "Telefone": "", "Obs": ""}])

# ---------------- Catálogo / Estoque ----------------
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_COMPRAS = "Compras"
ABA_AJUSTES = "Ajustes"
ABA_MOVS   = "MovimentosEstoque"
ABA_FIADO  = "Fiado"

COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]

def _build_maps_e_estoque():
    try:
        dfp = carregar_aba(ABA_PROD)
    except Exception:
        st.error("Erro ao abrir a aba Produtos."); st.stop()
    col_id   = _first_col(dfp, ["ID","Codigo","Código","SKU"])
    col_nome = _first_col(dfp, ["Nome","Produto","Descrição"])
    col_preco= _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"])
    col_unid = _first_col(dfp, ["Unidade","Und"])
    col_custo= _first_col(dfp, ["Custo","PreçoCusto","PrecoCusto","CustoUnit","Custo Unidade"])
    col_foto = _first_col(dfp, ["Foto","Imagem","Image","Photo","FotoURL","ImagemURL"])
    if not col_id or not col_nome:
        st.error("A aba Produtos precisa ter colunas de ID e Nome."); st.stop()

    # usar só nome como label
    dfp["_label"] = dfp[col_nome].astype(str).fillna("").str.strip()
    dup_counts = {}
    def _dedupe(lbl):
        c = dup_counts.get(lbl, 0)
        dup_counts[lbl] = c + 1
        return lbl if c == 0 else f"{lbl} ({c+1})"
    dfp["_label"] = dfp["_label"].map(_dedupe)

    cat_map = dfp.set_index("_label")[[col_id, col_nome, col_preco, col_unid, col_foto]].to_dict("index")
    labels = ["(selecione)"] + sorted(cat_map.keys(), key=lambda x: x.lower())
    id_to_name, id_to_cost, id_to_stock, id_to_img = {}, {}, {}, {}
    for _, r in dfp.iterrows():
        pid = str(r[col_id]).strip()
        if pid:
            id_to_name[pid] = str(r.get(col_nome,"") or "").strip()
            if col_custo: id_to_cost[pid] = _to_num(r.get(col_custo))
            if col_foto: id_to_img[pid] = str(r.get(col_foto,"") or "").strip()

    return dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, col_nome, col_preco, col_unid, id_to_img

dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, col_nome, col_preco, col_unid, id_to_img = _build_maps_e_estoque()

# ================= Estado inicial =================
if "cart" not in st.session_state: st.session_state["cart"] = []

# ================= Carrinho =================
st.subheader("Nova venda / cupom")

with st.form("add_item"):
    sel = st.selectbox("Produto", labels, index=0)
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        qtd = st.number_input("Qtd", min_value=1, step=1, value=1)
    with c2:
        preco_sug = 0.0
        if sel != "(selecione)" and col_preco:
            preco_sug = _to_num(cat_map[sel].get(col_preco))
        preco = st.number_input("Preço unitário (R$)", min_value=0.0, value=float(preco_sug), step=0.1, format="%.2f")
    with c3:
        unid_show = cat_map[sel].get(col_unid) if sel != "(selecione)" and col_unid else "un"
        st.text_input("Unidade", value=str(unid_show), disabled=True)
    add = st.form_submit_button("➕ Adicionar ao carrinho", use_container_width=True)

if add and sel != "(selecione)":
    info = cat_map[sel]
    pid = str(info[col_id])
    st.session_state["cart"].append({
        "id": pid,
        "nome": str(info[col_nome]),
        "unid": str(info.get(col_unid, "un")),
        "foto": str(info.get("Foto") or info.get("Imagem") or ""),
        "qtd": int(qtd),
        "preco": float(preco)
    })
    st.success("Item adicionado.")

st.subheader("Carrinho")
if not st.session_state["cart"]:
    st.info("Nenhum item no carrinho.")
else:
    for idx, it in enumerate(st.session_state["cart"]):
        c0, c1, c2, c3 = st.columns([1,2,2,2])
        if it.get("foto"):
            c0.image(it["foto"], width=60)
        c1.write(f"**{it['nome']}**")
        c2.write(f"Qtd: {it['qtd']}")
        c3.write(f"Preço: {_fmt_brl_num(it['preco'])}")
