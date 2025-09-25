# pages/00_vendas.py — Vendas rápidas (carrinho + histórico/estorno/duplicar)
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import unicodedata
import unicodedata as _ud
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

import gspread
import pandas as pd
import requests  # Telegram
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

st.set_page_config(page_title="Vendas rápidas", page_icon="🧾", layout="wide")
st.title("🧾 Vendas rápidas (carrinho)")

# ================= Helpers básicos =================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    # remove control chars exceto \n\r\t
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key


def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc


@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)


@st.cache_data(ttl=10)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df


def _first_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    if df is None or df.empty:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def _to_num(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return 0.0
    # Heurística básica para milhares/ponto/virgula (BR)
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _fmt_brl_num(v: float) -> str:
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _gerar_id(prefixo: str = "F") -> str:
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"


def _garantir_aba(sh, nome: str, cols: List[str]):
    try:
        ws = sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10, len(cols)))
        ws.update("A1", [cols])
        return ws
    headers = ws.row_values(1) or []
    headers = [h.strip() for h in headers]
    falt = [c for c in cols if c not in headers]
    if falt:
        ws.update("A1", [headers + falt])
    return ws


def _append_rows(ws, rows: List[Dict[str, Any]]):
    headers = ws.row_values(1)
    hdr = [h.strip() for h in headers]
    to_append = []
    for d in rows:
        to_append.append([d.get(h, "") for h in hdr])
    if to_append:
        ws.append_rows(to_append, value_input_option="USER_ENTERED")


# -------- Validadores / Telegram --------
def _is_http_url(s: Any) -> bool:
    return isinstance(s, str) and s.strip().lower().startswith(("http://", "https://"))


def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False


def _tg_conf() -> Tuple[str, str]:
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return str(token or ""), str(chat_id or "")


def _tg_send(msg: str):
    if not _tg_enabled():
        return
    token, chat_id = _tg_conf()
    if not token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=8)
    except Exception:
        # Silencioso: não quebra fluxo de venda
        pass


def _tg_send_media_group(media: List[Dict[str, Any]]):
    """media: list of {'type':'photo','media':<url|file_id>,'caption':<html>,'parse_mode':'HTML'} (máx 10)"""
    if not _tg_enabled():
        return
    token, chat_id = _tg_conf()
    if not token or not chat_id or not media:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
        payload = {"chat_id": str(chat_id), "media": media[:10]}
        requests.post(url, json=payload, timeout=12)
    except Exception:
        pass


# ---------------- Clientes (normalização e dedupe) ----------------
ABA_CLIENTES = "Clientes"
COLS_CLIENTES = ["Cliente", "Telefone", "Obs"]


def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return "".join(ch for ch in _ud.normalize("NFD", s) if _ud.category(ch) != "Mn")


def _normalize_cliente(nome: str) -> str:
    nome = (nome or "").strip()
    nome = re.sub(r"\s+", " ", nome)
    return nome.title()


def _cliente_key(nome: str) -> str:
    base = _normalize_cliente(nome)
    base = _strip_accents(base).lower()
    return re.sub(r"\s+", " ", base).strip()


def _carregar_clientes() -> List[str]:
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        if dfc.empty:
            return []
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
    """Garante cadastro do cliente (sem duplicar variações)."""
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


# ---------------- Catálogo / Estoque / Custo ----------------
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_COMPRAS = "Compras"
ABA_AJUSTES = "Ajustes"
ABA_MOVS = "MovimentosEstoque"
ABA_FIADO = "Fiado"

COLS_FIADO = [
    "ID",
    "Data",
    "Cliente",
    "Valor",
    "Vencimento",
    "Status",
    "Obs",
    "DataPagamento",
    "FormaPagamento",
    "ValorPago",
]


def _build_maps_e_estoque():
    # Produtos
    try:
        dfp = carregar_aba(ABA_PROD)
    except Exception:
        st.error("Erro ao abrir a aba Produtos.")
        st.stop()

    col_id = _first_col(dfp, ["ID", "Codigo", "Código", "SKU"])
    col_nome = _first_col(dfp, ["Nome", "Produto", "Descrição"])
    col_preco = _first_col(dfp, ["PreçoVenda", "PrecoVenda", "Preço", "Preco"])
    col_unid = _first_col(dfp, ["Unidade", "Und"])
    col_custo = _first_col(dfp, ["Custo", "PreçoCusto", "PrecoCusto", "CustoUnit", "Custo Unidade"])
    col_foto = _first_col(dfp, ["Foto", "Imagem", "Image", "Photo", "FotoURL", "ImagemURL"])

    if not col_id or not col_nome:
        st.error("A aba Produtos precisa ter colunas de ID e Nome.")
        st.stop()

    # 👉 rótulo só com o NOME (sem ID). Se houver nomes repetidos, coloca (2), (3)...
    dfp["_label"] = dfp[col_nome].astype(str).fillna("").str.strip()
    dup_counts: Dict[str, int] = {}

    def _dedupe(lbl: str) -> str:
        c = dup_counts.get(lbl, 0)
        dup_counts[lbl] = c + 1
        return lbl if c == 0 else f"{lbl} ({c+1})"

    dfp["_label"] = dfp["_label"].map(_dedupe)

    # Mapa para o selectbox
    use_cols = [col_id, col_nome, col_preco, col_unid]
    if col_foto:
        use_cols.append(col_foto)
    cat_map = dfp.set_index("_label")[use_cols].to_dict("index")
    labels = ["(selecione)"] + sorted(cat_map.keys(), key=lambda x: x.lower())

    # Mapa nome/custo/foto
    id_to_name: Dict[str, str] = {}
    id_to_cost: Dict[str, float] = {}
    id_to_img: Dict[str, str] = {}

    for _, r in dfp.iterrows():
        pid = str(r[col_id]).strip()
        if not pid:
            continue
        id_to_name[pid] = str(r.get(col_nome, "") or "").strip()
        if col_custo:
            id_to_cost[pid] = _to_num(r.get(col_custo))
        if col_foto:
            id_to_img[pid] = str(r.get(col_foto, "") or "").strip()

    # ------- ESTOQUE = Entradas(Compras) - Saídas(Vendas líquidas) + Ajustes -------
    id_to_stock: Dict[str, float] = {}

    # Compras
    try:
        dcc = carregar_aba(ABA_COMPRAS)
    except Exception:
        dcc = pd.DataFrame()
    entradas: Dict[str, float] = {}
    if not dcc.empty:
        col_cc_pid = _first_col(dcc, ["IDProduto", "ProdutoID", "ID"])
        col_cc_qtd = _first_col(dcc, ["Qtd", "Quantidade"])
        col_cc_cus = _first_col(
            dcc, ["Custo Unitário", "CustoUnit", "CustoUnitário", "Custo Unit", "CustoUnitario", "CustoUnit"]
        )
        col_cc_dat = _first_col(dcc, ["Data"])
        if col_cc_pid and col_cc_qtd:
            for _, r in dcc.iterrows():
                pid = str(r.get(col_cc_pid, "")).strip()
                entradas[pid] = entradas.get(pid, 0.0) + _to_num(r.get(col_cc_qtd))
        # fallback custo: última compra
        if col_cc_pid and col_cc_cus:
            dcc["_dt"] = pd.to_datetime(dcc[col_cc_dat], format="%d/%m/%Y", errors="coerce") if col_cc_dat else pd.NaT
            dcc = dcc.sort_values("_dt")
            last_cost = dcc.groupby(col_cc_pid)[col_cc_cus].last()
            for pid, cus in last_cost.items():
                pid = str(pid)
                if pid and (pid not in id_to_cost or id_to_cost[pid] == 0):
                    id_to_cost[pid] = _to_num(cus)

    # Vendas
    try:
        dv = carregar_aba(ABA_VEND)
    except Exception:
        dv = pd.DataFrame()
    saidas: Dict[str, float] = {}
    if not dv.empty:
        col_v_pid = _first_col(dv, ["IDProduto", "ProdutoID", "ID"])
        col_v_qtd = _first_col(dv, ["Qtd", "Quantidade"])
        if col_v_pid and col_v_qtd:
            for _, r in dv.iterrows():
                pid = str(r.get(col_v_pid, "")).strip()
                saidas[pid] = saidas.get(pid, 0.0) + _to_num(r.get(col_v_qtd))  # estorno vem negativo

    # Ajustes
    try:
        daj = carregar_aba(ABA_AJUSTES)
    except Exception:
        daj = pd.DataFrame()
    ajustes: Dict[str, float] = {}
    if not daj.empty:
        col_aj_pid = _first_col(daj, ["ID", "IDProduto", "ProdutoID"])
        col_aj_qtd = _first_col(daj, ["Qtd", "Quantidade", "Qtde"])
        if col_aj_pid and col_aj_qtd:
            for _, r in daj.iterrows():
                pid = str(r.get(col_aj_pid, "")).strip()
                ajustes[pid] = ajustes.get(pid, 0.0) + _to_num(r.get(col_aj_qtd))

    for pid in set(list(entradas.keys()) + list(saidas.keys()) + list(ajustes.keys()) + list(id_to_name.keys())):
        e = entradas.get(pid, 0.0)
        s = saidas.get(pid, 0.0)
        a = ajustes.get(pid, 0.0)
        id_to_stock[pid] = e - s + a

    return dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, col_nome, col_preco, col_unid, id_to_img


# ====== carrega mapas ======
dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, col_nome, col_preco, col_unid, id_to_img = (
    _build_maps_e_estoque()
)

# ---------- Imagens: normalização segura ----------
def _extract_drive_id(u: str) -> str | None:
    """Extrai FILE_ID de padrões comuns do Google Drive."""
    if not isinstance(u, str) or not u:
        return None
    u = u.strip()
    # Caso seja só o ID (sem URL)
    if re.fullmatch(r"[A-Za-z0-9_-]{20,}", u):
        return u
    # Formatos comuns
    m = re.search(r"/file/d/([^/]+)/view", u)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([^&]+)", u)
    if m:
        return m.group(1)
    return None


def resolve_image_url(raw: Any) -> str | None:
    """
    Recebe um valor cru de 'foto' (URL/ID/etc.) e tenta produzir
    uma URL http(s) utilizável. Retorna None se não seguro/viável.
    """
    if not isinstance(raw, str):
        return None
    val = raw.strip()
    if not val:
        return None

    # Já é http(s)?
    if _is_http_url(val):
        # Drive 'open?id=' ou 'file/d/.../view' -> transformar
        if "drive.google.com" in val:
            fid = _extract_drive_id(val)
            if fid:
                return f"https://drive.google.com/uc?export=view&id={fid}"
            # Se não deu pra extrair, manter como está (pode funcionar)
            return val
        return val

    # Se for um ID “seco” do Drive
    fid = _extract_drive_id(val)
    if fid:
        return f"https://drive.google.com/uc?export=view&id={fid}"

    # Não consegui tornar exibível
    return None


# ---------- Render universal do item (carrinho/linhas) ----------
def _render_item_line_universal(x: dict, id_to_name_map: dict, stock_before_after: dict) -> str:
    pid = str(x.get("IDProduto") or x.get("ProdutoID") or x.get("ID") or x.get("id") or "?")
    qtd = int(_to_num(x.get("Qtd") if "Qtd" in x else x.get("qtd", 1)))
    preco = _to_num(x.get("PrecoUnit") if "PrecoUnit" in x else x.get("preco", 0))
    nome = id_to_name_map.get(pid, "Produto")
    subtotal = qtd * preco
    estoque_txt = ""
    if pid in stock_before_after:
        bef, aft = stock_before_after[pid]
        estoque_txt = f" — <i>estoque:</i> {int(bef)} → <b>{int(aft)}</b>"
    return f"• <b>{nome}</b> — x{qtd} @ {_fmt_brl_num(preco)} = <b>{_fmt_brl_num(subtotal)}</b>{estoque_txt}"


# ================= Estado inicial =================
if "cart" not in st.session_state:
    st.session_state["cart"] = []
if "forma" not in st.session_state:
    st.session_state["forma"] = "Dinheiro"
if "obs" not in st.session_state:
    st.session_state["obs"] = ""
if "data_venda" not in st.session_state:
    st.session_state["data_venda"] = date.today()
if "desc" not in st.session_state:
    st.session_state["desc"] = 0.0
if "cliente" not in st.session_state:
    st.session_state["cliente"] = ""
if "venc_fiado" not in st.session_state:
    st.session_state["venc_fiado"] = date.today() + timedelta(days=30)


# ================= Helper rerun =================
def _rerun():
    """Compatível com versões novas e antigas do Streamlit"""
    try:
        st.rerun()
    except AttributeError:
        try:
            st.experimental_rerun()  # type: ignore[attr-defined]
        except Exception:
            pass


# ================= Carrinho =================
st.subheader("Nova venda / cupom")

# Data
cdate, = st.columns(1)
with cdate:
    st.session_state["data_venda"] = st.date_input("Data da venda", value=st.session_state["data_venda"])

with st.form("add_item"):
    sel = st.selectbox("Produto", labels, index=0)
    c1, c2, c3 = st.columns([1, 1, 1])
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

if add:
    if sel == "(selecione)":
        st.warning("Selecione um produto.")
    else:
        info = cat_map[sel]
        # Observação: 'info' já contém col_foto se existir na planilha
        foto_raw = None
        # tenta pegar a coluna de foto dinamicamente
        for key in ("Foto", "Imagem", "Image", "Photo", "FotoURL", "ImagemURL"):
            if key in info:
                foto_raw = info.get(key)
                break

        st.session_state["cart"].append(
            {
                "id": str(info[col_id]),
                "nome": str(info[col_nome]),
                "unid": str(info.get(col_unid, "un")),
                "foto": str(foto_raw or ""),
                "qtd": int(qtd),
                "preco": float(preco),
            }
        )
        st.success("Item adicionado.")

# Tabela do carrinho (com foto)
st.subheader("Carrinho")
if not st.session_state["cart"]:
    st.info("Nenhum item no carrinho.")
else:
    for idx, it in enumerate(st.session_state["cart"]):
        c0, c1, c2, c3, c4, c5 = st.columns([1, 2.6, 1, 1.6, 1.8, 0.8])

        # imagem do produto (segura)
        url_img = resolve_image_url(it.get("foto", ""))
        if url_img:
            c0.image(url_img, width=54)
        else:
            c0.write("—")

        # nome
        c1.write(f"**{it['nome']}**")

        # estoque atual
        c2.caption(f"Estoque: {int(id_to_stock.get(it['id'], 0))}")

        # quantidade
        with c3:
            st.session_state["cart"][idx]["qtd"] = st.number_input(
                "Qtd", key=f"q_{idx}", min_value=1, step=1, value=int(it["qtd"])
            )

        # preço
        with c4:
            st.session_state["cart"][idx]["preco"] = st.number_input(
                "Preço (R$)", key=f"p_{idx}", min_value=0.0, step=0.1,
                value=float(it["preco"]), format="%.2f"
            )

        # remover item
        if c5.button("🗑️", key=f"rm_{idx}"):
            st.session_state["cart"].pop(idx)
            _rerun()  # força refresh imediato

