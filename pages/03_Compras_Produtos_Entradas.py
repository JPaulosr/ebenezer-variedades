# -*- coding: utf-8 -*-
# pages/03_compras_entradas.py — Registrar compras/entradas de estoque + Telegram + Fracionamento + Editar/Apagar
import json, unicodedata, re, time
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date
from pathlib import Path  # 👈 para st.page_link seguro

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
    if st.button("🔄 Atualizar dados", use_container_width=True):
        _refresh_now()

# ========= dados base =========
try:
    prod_df = _load_df(PRODUTOS_ABA, BUMP)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"): st.code(str(e))
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

# ------- estoque atual (Compras - Vendas + Ajustes) -------
def _estoque_atual(pid: str="", nome: str="") -> float:
    pid = (pid or "").strip(); nome = (nome or "").strip()

    def _sum(df, col_q, filtro):
        if df.empty or not col_q: return 0.0
        try:
            sub = df[filtro].copy()
            if sub.empty: return 0.0
            return pd.to_numeric(
                sub[col_q].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
                errors="coerce"
            ).fillna(0).sum()
        except Exception:
            return 0.0

    try: comp = _load_df(COMPRAS_ABA, BUMP)
    except Exception: comp = pd.DataFrame()
    try: vend = _load_df(VENDAS_ABA,  BUMP)
    except Exception: vend = pd.DataFrame()
    try: ajus = _load_df(AJUSTES_ABA,  BUMP)
    except Exception: ajus = pd.DataFrame()

    # Compras
    c_pid = _pick_col(comp, ["IDProduto","ProdutoID","ID"])
    c_qtd = _pick_col(comp, ["Qtd","Quantidade"])
    c_nom = _pick_col(comp, ["Produto","Nome"])
    ent = 0.0
    if c_qtd:
        if pid and c_pid: ent += _sum(comp, c_qtd, comp[c_pid].astype(str).str.strip()==pid)
        if nome and c_nom:
            ent += _sum(comp, c_qtd, (comp[c_nom].astype(str).str.strip()==nome) & (False if not c_pid else comp[c_pid].astype(str).str.strip().eq("").fillna(True)))

    # Vendas
    v_pid = _pick_col(vend, ["IDProduto","ProdutoID","ID"])
    v_qtd = _pick_col(vend, ["Qtd","Quantidade"])
    sai = 0.0
    if v_qtd and v_pid and pid:
        sai += _sum(vend, v_qtd, vend[v_pid].astype(str).str.strip()==pid)

    # Ajustes
    a_pid = _pick_col(ajus, ["ID","IDProduto","ProdutoID"])
    a_qtd = _pick_col(ajus, ["Qtd","Quantidade","Qtde"])
    aj = 0.0
    if a_qtd:
        if pid and a_pid: aj += _sum(ajus, a_qtd, ajus[a_pid].astype(str).str.strip()==pid)
        elif nome:
            an = _pick_col(ajus, ["Produto","Nome"])
            if an: aj += _sum(ajus, a_qtd, ajus[an].astype(str).str.strip()==nome)

    return float(ent - sai + aj)

# =========================
# ENTRADA DE COMPRAS
# =========================
UNIDADES_PADRAO = ["un","L","kg","g","ml","cx","pct","Outro"]
def _opt_index(val: str, options: list[str]) -> int:
    v = (val or "").strip()
    return options.index(v) if v in options else 0

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
    with c4:
        idx_unid = _opt_index(unid_sug or "un", UNIDADES_PADRAO)
        unid_sel = st.selectbox("Unidade", options=UNIDADES_PADRAO, index=idx_unid, help="Selecione a medida; escolha 'Outro' para digitar.")
    unid_outro = ""
    if unid_sel == "Outro":
        unid_outro = st.text_input("Se 'Outro'… qual medida?", placeholder="ex.: rolo, m, par")
    unid = (unid_outro.strip() if unid_sel == "Outro" else unid_sel)

    fornecedor = st.text_input("Fornecedor", value=forn_sug)
    obs        = st.text_input("Observações (opcional)")
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

st.divider()

# ========== Links de navegação (seguros) ==========
def _safe_page_link(path: str, label: str, icon: str):
    if Path(path).exists():
        st.page_link(path, label=label, icon=icon)
    else:
        st.caption(f"⚠️ Página não encontrada: `{path}`")

_safe_page_link("pages/02_cadastrar_produto.py", label="↩️ Voltar ao Cadastro/Editar", icon="➕")
_safe_page_link("pages/01_produtos.py", label="📦 Ir ao Catálogo", icon="📦")

# =========================
# 🧪 Fracionar granel → fracionados
# =========================
st.divider()
st.subheader("🧪 Fracionar — converter GRANEL (L) em fracionados")

# helper: última compra do granel (info rápida)
def _ultima_compra(pid: str, nome: str):
    try: comp = _load_df(COMPRAS_ABA, BUMP)
    except Exception: return None
    if comp.empty: return None

    col_id = None
    for c in ["IDProduto","ProdutoID","ID"]:
        if c in comp.columns:
            col_id = c; break
    col_nome = "Produto" if "Produto" in comp.columns else None
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

# carregar produtos
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
                             format_func=lambda i: _fmt_opt(df_granel.iloc[i]))
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
                                 format_func=lambda i: _fmt_opt(df_un.iloc[i]))
            qtd_1 = st.number_input("Qtd frascos A", min_value=0, step=1, value=0)
            vol_1_l = st.number_input("Volume por frasco A (em L) — ex.: 1.0", min_value=0.0, step=0.1, value=1.0, format="%.3f")
        with c2:
            idx_2 = st.selectbox("SKU fracionado B (ex.: 500 ml)", options=range(len(df_un)),
                                 format_func=lambda i: _fmt_opt(df_un.iloc[i]), index=0)
            qtd_2 = st.number_input("Qtd frascos B", min_value=0, step=1, value=0)
            vol_2_l = st.number_input("Volume por frasco B (em L) — ex.: 0.5", min_value=0.0, step=0.1, value=0.5, format="%.3f")

        total_litros = (qtd_1 * vol_1_l) + (qtd_2 * vol_2_l)
        st.write(f"🔁 Litros a baixar do granel: **{_fmt_num(total_litros)} L**")

        confirmar = st.button("Registrar fracionamento", use_container_width=True)

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

            # ---- Telegram do fracionamento ----
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

# =========================
# ✏️ Editar / 🗑️ Apagar registros
# =========================
st.divider()
st.subheader("✏️ Editar / 🗑️ Apagar registros")

def _load_with_rownums(aba: str, headers: list[str]):
    ws = _ensure_ws(aba, headers)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    if df.empty:
        df = pd.DataFrame(columns=headers)
    df = df.fillna("")
    df["__Linha"] = (df.index + 2).astype(int)  # cabeçalho está na linha 1
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    cols = ["__Linha"] + [c for c in df.columns if c != "__Linha"]
    return df[cols].copy(), ws

def _save_df_over(ws, df: pd.DataFrame):
    df2 = df.drop(columns=[c for c in df.columns if c == "__Linha"], errors="ignore")
    ws.clear()
    set_with_dataframe(ws, df2, include_index=False, include_column_header=True, resize=True)

tab_edit_comp, tab_edit_mov = st.tabs(["🧾 Editar Compras", "📦 Editar Movimentos"])

# ---------- Editar / Apagar COMPRAS ----------
with tab_edit_comp:
    dfc, ws_c = _load_with_rownums(COMPRAS_ABA, COMPRAS_HEADERS)
    if dfc.empty:
        st.info("Sem compras registradas ainda.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            filt_prod = st.text_input("Filtrar por Produto (contém)", "")
        with c2:
            filt_data = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", "")

        mask = pd.Series(True, index=dfc.index)
        if filt_prod.strip():
            mask &= dfc["Produto"].astype(str).str.contains(filt_prod.strip(), case=False, na=False)
        if filt_data.strip():
            mask &= dfc["Data"].astype(str).str.contains(filt_data.strip(), case=False, na=False)

        dfc_view = dfc[mask].copy().sort_values("__Linha", ascending=False)
        st.dataframe(dfc_view, use_container_width=True, hide_index=True)

        linhas_opts = dfc_view["__Linha"].tolist()
        if linhas_opts:
            linha_sel = st.selectbox("Escolha a linha para editar/apagar", options=linhas_opts)
            row_cur = dfc.loc[dfc["__Linha"] == linha_sel].iloc[0].copy()

            with st.form("form_edit_compra"):
                st.markdown(f"**Linha {linha_sel}** — edite os campos e salve")
                e1, e2, e3 = st.columns(3)
                with e1:
                    data_n = st.text_input("Data", row_cur.get("Data",""))
                    prod_n = st.text_input("Produto", row_cur.get("Produto",""))
                    unid_n = st.text_input("Unidade", row_cur.get("Unidade",""))
                with e2:
                    forn_n = st.text_input("Fornecedor", row_cur.get("Fornecedor",""))
                    qtd_n  = st.text_input("Qtd", row_cur.get("Qtd",""))
                    custo_n= st.text_input("Custo Unitário", row_cur.get("Custo Unitário",""))
                with e3:
                    total_n= st.text_input("Total", row_cur.get("Total",""))
                    idp_n  = st.text_input("IDProduto", row_cur.get("IDProduto",""))
                    obs_n  = st.text_input("Obs", row_cur.get("Obs",""))

                colb1, colb2 = st.columns([1,1])
                salvar_ed = colb1.form_submit_button("💾 Salvar alterações", use_container_width=True)
                apagar_ln = colb2.form_submit_button("🗑️ Apagar esta linha", use_container_width=True)

            if salvar_ed:
                idx_real = dfc.index[dfc["__Linha"] == linha_sel][0]
                for k, v in {
                    "Data": data_n, "Produto": prod_n, "Unidade": unid_n, "Fornecedor": forn_n,
                    "Qtd": qtd_n, "Custo Unitário": custo_n, "Total": total_n,
                    "IDProduto": idp_n, "Obs": obs_n
                }.items():
                    dfc.at[idx_real, k] = v
                _save_df_over(ws_c, dfc)
                st.success(f"Linha {linha_sel} salva.")
                st.cache_data.clear()
                st.rerun()

            if apagar_ln:
                dfc_drop = dfc[dfc["__Linha"] != linha_sel].copy()
                _save_df_over(ws_c, dfc_drop)
                st.success(f"Linha {linha_sel} apagada.")
                st.cache_data.clear()
                st.rerun()

# ---------- Editar / Apagar MOVIMENTOS ----------
with tab_edit_mov:
    dfm, ws_m = _load_with_rownums(MOVS_ABA, MOV_HEADERS)
    if dfm.empty:
        st.info("Sem movimentos registrados ainda.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            filt_prod_m = st.text_input("Filtrar por Produto (contém)", "")
        with c2:
            filt_data_m = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", "")

        maskm = pd.Series(True, index=dfm.index)
        if filt_prod_m.strip():
            maskm &= dfm["Produto"].astype(str).str.contains(filt_prod_m.strip(), case=False, na=False)
        if filt_data_m.strip():
            maskm &= dfm["Data"].astype(str).str.contains(filt_data_m.strip(), case=False, na=False)

        dfm_view = dfm[maskm].copy().sort_values("__Linha", ascending=False)
        st.dataframe(dfm_view, use_container_width=True, hide_index=True)

        linhas_opts_m = dfm_view["__Linha"].tolist()
        if linhas_opts_m:
            linha_sel_m = st.selectbox("Escolha a linha para editar/apagar (Movimentos)", options=linhas_opts_m)

            row_mov = dfm.loc[dfm["__Linha"] == linha_sel_m].iloc[0].copy()
            with st.form("form_edit_mov"):
                st.markdown(f"**Linha {linha_sel_m}** — edite campos e salve")
                e1, e2, e3 = st.columns(3)
                with e1:
                    data2  = st.text_input("Data", row_mov.get("Data",""))
                    idp2   = st.text_input("IDProduto", row_mov.get("IDProduto",""))
                    prod2  = st.text_input("Produto", row_mov.get("Produto",""))
                with e2:
                    tipo2  = st.text_input("Tipo", row_mov.get("Tipo",""))
                    qtd2   = st.text_input("Qtd", row_mov.get("Qtd",""))
                    obs2   = st.text_input("Obs", row_mov.get("Obs",""))
                with e3:
                    id2    = st.text_input("ID", row_mov.get("ID",""))
                    doc2   = st.text_input("Documento/NF", row_mov.get("Documento/NF",""))
                    org2   = st.text_input("Origem", row_mov.get("Origem",""))
                saldo2 = st.text_input("SaldoApós", row_mov.get("SaldoApós",""))

                colb1, colb2 = st.columns([1,1])
                salvar_ed2 = colb1.form_submit_button("💾 Salvar alterações", use_container_width=True)
                apagar_ln2 = colb2.form_submit_button("🗑️ Apagar esta linha", use_container_width=True)

            if salvar_ed2:
                idx_real = dfm.index[dfm["__Linha"] == linha_sel_m][0]
                for k, v in {
                    "Data": data2, "IDProduto": idp2, "Produto": prod2, "Tipo": tipo2,
                    "Qtd": qtd2, "Obs": obs2, "ID": id2, "Documento/NF": doc2,
                    "Origem": org2, "SaldoApós": saldo2
                }.items():
                    dfm.at[idx_real, k] = v
                _save_df_over(ws_m, dfm)
                st.success(f"Linha {linha_sel_m} salva.")
                st.cache_data.clear()
                st.rerun()

            if apagar_ln2:
                dfm_drop = dfm[dfm["__Linha"] != linha_sel_m].copy()
                _save_df_over(ws_m, dfm_drop)
                st.success(f"Linha {linha_sel_m} apagada.")
                st.cache_data.clear()
                st.rerun()
