# -*- coding: utf-8 -*-
# pages/03_Compras_Produtos_Entradas.py — Compras/entradas de estoque + Telegram + Fracionamento + Edição/Exclusão
from __future__ import annotations

import json, unicodedata, re, time
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date, datetime
from typing import Iterable

st.set_page_config(page_title="Compras / Entradas", page_icon="🧾", layout="wide")
st.title("🧾 Compras / Entradas de Estoque")

# ---------------- Utils de refresh/cache ----------------
def _refresh_now():
    st.session_state["_refresh_ts"] = time.time()
    st.cache_data.clear()
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()

BUMP = st.session_state.get("_refresh_ts", 0)  # usado para invalidar cache

def _fmt_brl(v: float) -> str:
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _fmt_num(v: float, casas=3) -> str:
    return f"{float(v):.{casas}f}".replace(".", ",")

# ========= credenciais =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
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

@st.cache_data(ttl=120)
def _load_df(aba: str, _bump: float | None = None) -> pd.DataFrame:
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
    for col in cur.columns: row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

def _to_float(x):
    if x is None or str(x).strip()=="": return ""
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

# ========= Telegram =========
def _tg_enabled() -> bool:
    try: return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception: return False

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

# ========= abas/headers =========
PRODUTOS_ABA = "Produtos"
COMPRAS_ABA  = "Compras"
VENDAS_ABA   = "Vendas"
AJUSTES_ABA  = "Ajustes"
MOVS_ABA     = "MovimentosEstoque"

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# ---------- Botão Atualizar ----------
c_at, _ = st.columns([1, 6])
with c_at:
    if st.button("🔄 Atualizar dados", use_container_width=True, key=f"btn_atualizar_{BUMP}"):
        _refresh_now()

# ========= dados base =========
try:
    prod_df = _load_df(PRODUTOS_ABA, BUMP)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"): st.code(str(e))
    st.stop()

def _pick_col(df: pd.DataFrame, cands: Iterable[str] ) -> str | None:
    for c in cands:
        if c in df.columns: return c
    return None

COL = {
    "id":   _pick_col(prod_df, ["ID","Id","id","Codigo","Código","SKU"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "forn": _pick_col(prod_df, ["Fornecedor","FornecedorNome"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid","Und"]),
}

# ======== ESTOQUE ATUAL — PADRÃO ÚNICO: MovimentosEstoque ========
def _estoque_atual(pid: str = "", nome: str = "") -> float:
    """
    Calcula estoque atual SOMENTE a partir da aba MovimentosEstoque,
    para ficar consistente com a página 'Estoque' e refletir fracionamentos.
    """
    pid = (pid or "").strip()
    nome = (nome or "").strip()

    try:
        mov = _load_df(MOVS_ABA, BUMP)
    except Exception:
        mov = pd.DataFrame()

    if mov.empty:
        return 0.0

    col_id   = _pick_col(mov, ["IDProduto", "ProdutoID", "ID"])
    col_nome = _pick_col(mov, ["Produto", "Nome"])
    col_qtd  = _pick_col(mov, ["Qtd", "Quantidade", "Qtde"])
    col_tipo = _pick_col(mov, ["Tipo"])

    if not col_qtd or not col_tipo:
        return 0.0

    df = mov.copy()
    if pid and col_id:
        df = df[df[col_id].astype(str).str.strip() == pid]
    elif nome and col_nome:
        df = df[df[col_nome].astype(str).str.strip() == nome]

    if df.empty:
        return 0.0

    def _num(x: str) -> float:
        s = str(x or "").strip().replace(" ", "")
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    def _sign(tipo: str) -> int:
        t = (tipo or "").lower()
        if "+" in t: return +1
        if "-" in t: return -1
        if t.startswith("b"):  # B entrada
            return +1
        if t.startswith("v"):  # V venda
            return -1
        if t.startswith("a"):  # A ajuste (+/- no texto)
            if "-" in t: return -1
            if "+" in t: return +1
            return 0
        return 0

    qtd = 0.0
    for _, r in df.iterrows():
        qtd += _num(r.get(col_qtd, 0)) * _sign(r.get(col_tipo, ""))

    return float(qtd)

# =========================
# ENTRADA DE COMPRAS
# =========================
UNIDADES_PADRAO = ["un","L","kg","g","ml","cx","pct","Outro"]
def _opt_index(val: str, options: list[str]) -> int:
    v = (val or "").strip()
    return options.index(v) if v in options else 0

st.subheader("Nova compra / entrada")
with st.form("form_compra"):
    usar_lista = st.checkbox("Selecionar produto da lista", value=True, key=f"usar_lista_{BUMP}")
    if usar_lista:
        if prod_df.empty:
            st.warning("Sem produtos cadastrados."); st.stop()

        def _fmt(r):
            n = _nz(r.get(COL["nome"], "")) or "(sem nome)"
            f = _nz(r.get(COL["forn"], ""))
            return n + (f" — " + f if f else "")

        labels = prod_df.apply(_fmt, axis=1).tolist()
        idx = st.selectbox("Produto", options=range(len(prod_df)),
                           format_func=lambda i: labels[i], key=f"sel_prod_{BUMP}")
        row = prod_df.iloc[idx]
        prod_nome = _nz(row.get(COL["nome"], ""))
        prod_id   = _nz(row.get(COL["id"], ""))
        unid_sug  = _nz(row.get(COL["unid"], ""))
        forn_sug  = _nz(row.get(COL["forn"], ""))
    else:
        prod_nome = st.text_input("Produto (nome exato)", key=f"t_prod_{BUMP}")
        prod_id   = st.text_input("ID (opcional)", key=f"t_id_{BUMP}")
        unid_sug  = ""
        forn_sug  = ""

    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1: data_c = st.date_input("Data da compra", value=date.today(), key=f"dt_{BUMP}")
    with c2: qtd    = st.text_input("Qtd", placeholder="Ex.: 10", key=f"qtd_{BUMP}")
    with c3: custo  = st.text_input("Custo unitário (R$)", placeholder="Ex.: 12,50", key=f"custo_{BUMP}")
    with c4:
        idx_unid = _opt_index(unid_sug or "un", UNIDADES_PADRAO)
        unid_sel = st.selectbox("Unidade", options=UNIDADES_PADRAO, index=idx_unid,
                                help="Selecione a medida; escolha 'Outro' para digitar.", key=f"unid_{BUMP}")
    unid_outro = ""
    if unid_sel == "Outro":
        unid_outro = st.text_input("Se 'Outro'… qual medida?", placeholder="ex.: rolo, m, par", key=f"unid_outro_{BUMP}")
    unid = (unid_outro.strip() if unid_sel == "Outro" else unid_sel)

    fornecedor = st.text_input("Fornecedor", value=forn_sug, key=f"forn_{BUMP}")
    obs        = st.text_input("Observações (opcional)", key=f"obs_{BUMP}")
    salvar     = st.form_submit_button("➕ Registrar entrada", use_container_width=True)

if salvar:
    if not prod_nome.strip():
        st.error("Selecione ou digite um produto."); st.stop()
    if (unid_sel == "Outro") and not unid.strip():
        st.error("Informe a unidade em 'Outro'."); st.stop()

    qtd_f = _to_float(qtd); cst_f = _to_float(custo)
    if qtd_f in ("", None) or cst_f in ("", None):
        st.error("Preencha **Qtd** e **Custo unitário**."); st.stop()

    estoque_antes  = _estoque_atual(pid=_nz(prod_id), nome=_nz(prod_nome))
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
        "SaldoApós": str(int(estoque_depois)) if float(estoque_depois).is_integer() else str(estoque_depois).replace(",", ".")
    })

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
    _refresh_now()

# =========================
# 🚪 Navegação segura (sem PageNotFound)
# =========================
st.divider()
c_nav1, c_nav2 = st.columns(2)

def _try_switch(candidates: list[str]) -> bool:
    for cand in candidates:
        try:
            st.switch_page(cand)
            return True
        except Exception:
            pass
    return False

with c_nav1:
    if st.button("↩️ Voltar ao Cadastro/Editar", use_container_width=True, key=f"btn_voltar_{BUMP}"):
        ok = _try_switch([
            "pages/02_cadastrar_produto.py",
            "pages/02_Cadastrar_Produto.py",
            "02_cadastrar_produto.py",
        ])
        if not ok:
            st.warning("Página de cadastro/edição não encontrada. Verifique o nome do arquivo em /pages.")

with c_nav2:
    if st.button("📦 Ir ao Catálogo", use_container_width=True, key=f"btn_catalogo_{BUMP}"):
        ok = _try_switch([
            "pages/01_produtos.py",
            "pages/01_Produtos.py",
            "01_produtos.py",
        ])
        if not ok:
            st.warning("Página do catálogo não encontrada. Confirme o nome do arquivo em /pages.")

# =========================
# 🧪 Fracionar granel → fracionados
# =========================
st.divider()
st.subheader("🧪 Fracionar — converter GRANEL (L) em fracionados")

def _ultima_compra(pid: str, nome: str):
    try: comp = _load_df(COMPRAS_ABA, BUMP)
    except Exception: return None
    if comp.empty: return None

    col_id = _pick_col(comp, ["IDProduto","ProdutoID","ID"])
    col_nome = _pick_col(comp, ["Produto","Nome"])
    col_data = "Data" if "Data" in comp.columns else None

    df = comp.copy()
    if col_id:
        df = df[df[col_id].astype(str).str.strip() == str(pid).strip()]
    elif col_nome:
        df = df[df[col_nome].astype(str).str.strip() == str(nome).strip()]

    if df.empty: return None
    if col_data and col_data in df.columns:
        try:
            df["_d"] = pd.to_datetime(df[col_data], format="%d/%m/%Y", errors="coerce")
            df = df.sort_values("_d", ascending=False)
        except Exception: pass

    row = df.iloc[0].to_dict()
    return {
        "data": row.get("Data",""),
        "qtd": row.get("Qtd",""),
        "unid": row.get("Unidade",""),
        "custo_unit": row.get("Custo Unitário",""),
        "total": row.get("Total","")
    }

try:
    produtos = _load_df(PRODUTOS_ABA, BUMP)
except Exception:
    produtos = pd.DataFrame(columns=["ID","Nome","Unidade"])

COL_ID   = COL["id"] or "ID"
COL_NOME = COL["nome"] or "Nome"
COL_UNID = COL["unid"] or "Unidade"

if produtos.empty or COL_UNID not in produtos.columns:
    st.info("Cadastre produtos primeiro (incluindo um SKU granel em **L**).")
else:
    df_granel = produtos[produtos[COL_UNID].astype(str).str.strip().str.lower().eq("l")].copy()
    df_un     = produtos[produtos[COL_UNID].astype(str).str.strip().str.lower().eq("un")].copy()

    if df_granel.empty:
        st.warning("Nenhum produto granel (Unidade = L) encontrado.")
    elif df_un.empty:
        st.warning("Nenhum produto fracionado (Unidade = un) encontrado.")
    else:
        def _fmt_opt(r):
            return f"{_nz(r.get(COL_NOME,''))}  ·  {_nz(r.get(COL_ID,''))}".strip()

        idx_g = st.selectbox("Matéria-prima (granel em L)", options=range(len(df_granel)),
                             format_func=lambda i: _fmt_opt(df_granel.iloc[i]), key=f"sel_granel_{BUMP}")
        row_g = df_granel.iloc[idx_g]
        gid   = _nz(row_g.get(COL_ID,""))
        gnome = _nz(row_g.get(COL_NOME,""))

        estoque_g = _estoque_atual(pid=gid, nome=gnome)
        st.caption(f"📦 Estoque atual (granel): {estoque_g if isinstance(estoque_g,(int,float)) else 0} L")

        info = _ultima_compra(gid, gnome)
        if info:
            st.caption(f"🧾 Última compra: {info['data']} · Qtd {info['qtd']} {info['unid']} · Custo unit {info['custo_unit']} · Total {info['total']}")

        c1, c2 = st.columns(2)
        with c1:
            idx_1 = st.selectbox("SKU fracionado A (ex.: 1 L)", options=range(len(df_un)),
                                 format_func=lambda i: _fmt_opt(df_un.iloc[i]), key=f"sel_frac_a_{BUMP}")
            qtd_1 = st.number_input("Qtd frascos A", min_value=0, step=1, value=0, key=f"qtd_a_{BUMP}")
            vol_1_l = st.number_input("Volume por frasco A (em L) — ex.: 1.0", min_value=0.0, step=0.1,
                                      value=1.0, format="%.3f", key=f"vol_a_{BUMP}")
        with c2:
            idx_2 = st.selectbox("SKU fracionado B (ex.: 500 ml)", options=range(len(df_un)),
                                 format_func=lambda i: _fmt_opt(df_un.iloc[i]), index=0, key=f"sel_frac_b_{BUMP}")
            qtd_2 = st.number_input("Qtd frascos B", min_value=0, step=1, value=0, key=f"qtd_b_{BUMP}")
            vol_2_l = st.number_input("Volume por frasco B (em L) — ex.: 0.5", min_value=0.0, step=0.1,
                                      value=0.5, format="%.3f", key=f"vol_b_{BUMP}")

        total_litros = (qtd_1 * vol_1_l) + (qtd_2 * vol_2_l)
        st.write(f"🔁 Litros a baixar do granel: **{_fmt_num(total_litros)} L**")

        confirmar = st.button("Registrar fracionamento", use_container_width=True, key=f"btn_frac_{BUMP}")

        if confirmar:
            if total_litros <= 0:
                st.error("Informe quantidades > 0 para fracionar."); st.stop()
            if isinstance(estoque_g, (int, float)) and estoque_g < total_litros - 1e-9:
                st.error("Estoque do granel insuficiente para este fracionamento."); st.stop()

            ws_mov = _ensure_ws(MOVS_ABA, MOV_HEADERS)
            data_str = date.today().strftime("%d/%m/%Y")

            # saída do granel (litros, negativa)
            _append_row(ws_mov, {
                "Data": data_str,
                "IDProduto": gid,
                "Produto": gnome,
                "Tipo": "C fracionamento -",
                "Qtd": str(total_litros).replace(".", ","),
                "Obs": "Fracionamento para SKUs vendáveis",
                "ID": "",
                "Documento/NF": "",
                "Origem": "Fracionamento",
                "SaldoApós": ""
            })

            linhas = []
            # entrada fracionado A (unidades)
            if qtd_1 > 0:
                r1 = df_un.iloc[idx_1]
                _append_row(ws_mov, {
                    "Data": data_str,
                    "IDProduto": _nz(r1.get(COL_ID,"")),
                    "Produto": _nz(r1.get(COL_NOME,"")),
                    "Tipo": "C fracionamento +",
                    "Qtd": str(qtd_1),
                    "Obs": f"Fracionamento: {_fmt_num(vol_1_l)} L/frasco",
                    "ID": "",
                    "Documento/NF": "",
                    "Origem": "Fracionamento",
                    "SaldoApós": ""
                })
                linhas.append(f"• {_nz(r1.get(COL_NOME,''))}: <b>{qtd_1}</b> un ({_fmt_num(vol_1_l)} L/frasco)")

            # entrada fracionado B (unidades)
            if qtd_2 > 0:
                r2 = df_un.iloc[idx_2]
                _append_row(ws_mov, {
                    "Data": data_str,
                    "IDProduto": _nz(r2.get(COL_ID,"")),
                    "Produto": _nz(r2.get(COL_NOME,"")),
                    "Tipo": "C fracionamento +",
                    "Qtd": str(qtd_2),
                    "Obs": f"Fracionamento: {_fmt_num(vol_2_l)} L/frasco",
                    "ID": "",
                    "Documento/NF": "",
                    "Origem": "Fracionamento",
                    "SaldoApós": ""
                })
                linhas.append(f"• {_nz(r2.get(COL_NOME,''))}: <b>{qtd_2}</b> un ({_fmt_num(vol_2_l)} L/frasco)")

            saldo_depois = (estoque_g - total_litros) if isinstance(estoque_g, (int,float)) else None
            msg = (
                "🧪 <b>Fracionamento registrado</b>\n"
                f"{data_str}\n"
                f"Granel: <b>{gnome}</b>  ↓ <b>{_fmt_num(total_litros)} L</b>\n"
                + ("\n".join(linhas) + "\n" if linhas else "")
                + (f"📦 Granel: {_fmt_num(estoque_g)} → <b>{_fmt_num(saldo_depois)}</b> L" if saldo_depois is not None else "")
            )
            _tg_send(msg)

            st.success("Fracionamento registrado com sucesso! ✅")
            st.toast("Movimentos de fracionamento lançados", icon="✅")
            _refresh_now()

# =========================================================
# ✏️ Editar / 🗑️ Apagar registros (Compras & Movimentos)
# =========================================================
st.divider()
st.subheader("✏️ Editar / 🗑️ Apagar registros")

def _load_with_rownums(aba: str, headers: list[str]):
    ws = _ensure_ws(aba, headers)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    if df.empty:
        df = pd.DataFrame(columns=headers)
    df = df.fillna("")
    df["__Linha"] = (df.index + 2).astype(int)  # cabeçalho = 1
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    cols = ["__Linha"] + [c for c in df.columns if c != "__Linha"]
    return df[cols].copy(), ws

def _save_df_over(ws, df: pd.DataFrame):
    df2 = df.drop(columns=[c for c in df.columns if c == "__Linha"], errors="ignore")
    ws.clear()
    set_with_dataframe(ws, df2.fillna(""), include_index=False, include_column_header=True, resize=True)

def _contains(series: pd.Series, text: str) -> pd.Series:
    if not text: return pd.Series([True]*len(series), index=series.index)
    s = series.astype(str).str.normalize('NFKD').str.lower()
    t = str(text).strip().lower()
    return s.str.contains(re.escape(t), na=False)

tab_edit_comp, tab_edit_mov = st.tabs(["Editar Compras", "Editar Movimentos"])

with tab_edit_comp:
    df, ws = _load_with_rownums(COMPRAS_ABA, COMPRAS_HEADERS)

    c1, c2 = st.columns([1,1])
    with c1:
        f_prod = st.text_input("Filtrar por Produto (contém)", key=f"fprod_comp_{BUMP}")
    with c2:
        f_data = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", key=f"fdata_comp_{BUMP}")

    mask = _contains(df["Produto"], f_prod) & _contains(df["Data"], f_data)
    df_view = df[mask].reset_index(drop=True)

    st.caption(f"{len(df_view)} registro(s) encontrados")
    st.dataframe(df_view, use_container_width=True, hide_index=True)

    if not df_view.empty:
        idx = st.number_input("Selecione o índice da linha para editar/apagar (0 = primeira da tabela acima)",
                              min_value=0, max_value=len(df_view)-1, step=1, value=0,
                              key=f"idx_edit_comp_{BUMP}")
        rec = df_view.iloc[int(idx)].to_dict()
        st.markdown("**Registro selecionado:**")
        st.json(rec)

        with st.form(f"form_edit_comp_{BUMP}"):
            c1, c2, c3 = st.columns(3)
            with c1:
                e_data = st.text_input("Data", value=_nz(rec.get("Data","")))
                e_prod = st.text_input("Produto", value=_nz(rec.get("Produto","")))
                e_unid = st.text_input("Unidade", value=_nz(rec.get("Unidade","")))
            with c2:
                e_forn = st.text_input("Fornecedor", value=_nz(rec.get("Fornecedor","")))
                e_qtd  = st.text_input("Qtd", value=_nz(rec.get("Qtd","")))
                e_cu   = st.text_input("Custo Unitário", value=_nz(rec.get("Custo Unitário","")))
            with c3:
                e_total = st.text_input("Total", value=_nz(rec.get("Total","")))
                e_idp   = st.text_input("IDProduto", value=_nz(rec.get("IDProduto","")))
                e_obs   = st.text_input("Obs", value=_nz(rec.get("Obs","")))

            col_s1, col_s2 = st.columns([1,1])
            with col_s1:
                salvar = st.form_submit_button("💾 Salvar alterações", use_container_width=True)
            with col_s2:
                apagar = st.form_submit_button("🗑️ Apagar registro", use_container_width=True)

        if salvar or apagar:
            linha_real = int(rec["__Linha"])
            base = df.copy()
            pos = base.index[base["__Linha"] == linha_real]
            if len(pos) != 1:
                st.error("Não foi possível localizar a linha na planilha.")
            else:
                base_idx = pos[0]
                if apagar:
                    base = base.drop(index=base_idx).reset_index(drop=True)
                    _save_df_over(ws, base)
                    st.success(f"Registro da linha {linha_real} apagado.")
                    _refresh_now()
                else:
                    base.at[base_idx, "Data"] = e_data
                    base.at[base_idx, "Produto"] = e_prod
                    base.at[base_idx, "Unidade"] = e_unid
                    base.at[base_idx, "Fornecedor"] = e_forn
                    base.at[base_idx, "Qtd"] = e_qtd
                    base.at[base_idx, "Custo Unitário"] = e_cu
                    base.at[base_idx, "Total"] = e_total
                    base.at[base_idx, "IDProduto"] = e_idp
                    base.at[base_idx, "Obs"] = e_obs
                    _save_df_over(ws, base)
                    st.success(f"Registro da linha {linha_real} atualizado.")
                    _refresh_now()

with tab_edit_mov:
    dfm, wsm = _load_with_rownums(MOVS_ABA, MOV_HEADERS)

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        f_prod_m = st.text_input("Filtrar por Produto (contém)", key=f"fprod_mov_{BUMP}")
    with c2:
        f_tipo_m = st.text_input("Filtrar por Tipo (contém)", key=f"ftipo_mov_{BUMP}")
    with c3:
        f_data_m = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", key=f"fdata_mov_{BUMP}")

    maskm = _contains(dfm["Produto"], f_prod_m) & _contains(dfm["Tipo"], f_tipo_m) & _contains(dfm["Data"], f_data_m)
    dfm_view = dfm[maskm].reset_index(drop=True)

    st.caption(f"{len(dfm_view)} registro(s) encontrados")
    st.dataframe(dfm_view, use_container_width=True, hide_index=True)

    if not dfm_view.empty:
        idxm = st.number_input("Selecione o índice da linha para editar/apagar (0 = primeira da tabela acima)",
                               min_value=0, max_value=len(dfm_view)-1, step=1, value=0,
                               key=f"idx_edit_mov_{BUMP}")
        recm = dfm_view.iloc[int(idxm)].to_dict()
        st.markdown("**Registro selecionado:**")
        st.json(recm)

        with st.form(f"form_edit_mov_{BUMP}"):
            c1, c2, c3 = st.columns(3)
            with c1:
                m_data = st.text_input("Data", value=_nz(recm.get("Data","")))
                m_idp  = st.text_input("IDProduto", value=_nz(recm.get("IDProduto","")))
                m_prod = st.text_input("Produto", value=_nz(recm.get("Produto","")))
            with c2:
                m_tipo = st.text_input("Tipo", value=_nz(recm.get("Tipo","")))
                m_qtd  = st.text_input("Qtd", value=_nz(recm.get("Qtd","")))
                m_obs  = st.text_input("Obs", value=_nz(recm.get("Obs","")))
            with c3:
                m_id   = st.text_input("ID", value=_nz(recm.get("ID","")))
                m_doc  = st.text_input("Documento/NF", value=_nz(recm.get("Documento/NF","")))
                m_org  = st.text_input("Origem", value=_nz(recm.get("Origem","")))
            m_saldo = st.text_input("SaldoApós", value=_nz(recm.get("SaldoApós","")))

            col_s1, col_s2 = st.columns([1,1])
            with col_s1:
                salvarm = st.form_submit_button("💾 Salvar alterações", use_container_width=True)
            with col_s2:
                apagarm = st.form_submit_button("🗑️ Apagar registro", use_container_width=True)

        if salvarm or apagarm:
            linha_real_m = int(recm["__Linha"])
            basem = dfm.copy()
            posm = basem.index[basem["__Linha"] == linha_real_m]
            if len(posm) != 1:
                st.error("Não foi possível localizar a linha na planilha.")
            else:
                base_idx_m = posm[0]
                if apagarm:
                    basem = basem.drop(index=base_idx_m).reset_index(drop=True)
                    _save_df_over(wsm, basem)
                    st.success(f"Registro da linha {linha_real_m} apagado.")
                    _refresh_now()
                else:
                    basem.at[base_idx_m, "Data"] = m_data
                    basem.at[base_idx_m, "IDProduto"] = m_idp
                    basem.at[base_idx_m, "Produto"] = m_prod
                    basem.at[base_idx_m, "Tipo"] = m_tipo
                    basem.at[base_idx_m, "Qtd"] = m_qtd
                    basem.at[base_idx_m, "Obs"] = m_obs
                    basem.at[base_idx_m, "ID"] = m_id
                    basem.at[base_idx_m, "Documento/NF"] = m_doc
                    basem.at[base_idx_m, "Origem"] = m_org
                    basem.at[base_idx_m, "SaldoApós"] = m_saldo
                    _save_df_over(wsm, basem)
                    st.success(f"Registro da linha {linha_real_m} atualizado.")
                    _refresh_now()
