# -*- coding: utf-8 -*-
# pages/03_contagem_inicial.py — Contagem de estoque (definir nível) — alinhado ao 02_cadastrar_produto.py
import json, unicodedata, math, re, html
from datetime import datetime, timedelta, date

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # Telegram

st.set_page_config(page_title="Contagem de estoque", page_icon="📋", layout="wide")
st.title("📋 Contagem de estoque (definir nível)")

# =============================================================================
# Credenciais / Sheets (mesmo padrão do arquivo-base)
# =============================================================================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente em st.secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente em st.secrets."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10)
def _load_df(aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _safe_load(aba: str) -> pd.DataFrame:
    try:
        return _load_df(aba)
    except Exception:
        return pd.DataFrame()

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        # garante cabeçalho mínimo
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

def _msg_ok(msg):
    st.success(msg)
    try: st.cache_data.clear()
    except: pass

# =============================================================================
# Telegram — mesmo padrão do seu 02_cadastrar_produto.py
# =============================================================================
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
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=8)
    except Exception:
        pass

# =============================================================================
# Mapeamentos flexíveis (iguais ao base, com o que precisamos aqui)
# =============================================================================
def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _map_cols_produtos(df):
    return {
        "id":        _pick_col(df, ["ID","Id","id","Codigo","Código"]),
        "nome":      _pick_col(df, ["Nome","Produto","Descrição","Descricao"]),
        "categoria": _pick_col(df, ["Categoria","Grupo"]),
        "unidade":   _pick_col(df, ["Unidade","Unid"]),
        "forn":      _pick_col(df, ["Fornecedor","FornecedorNome","Fornecedor ID","FornecedorID"]),
        "custo":     _pick_col(df, ["CustoAtual","Custo","Custo Atual","CustoMedio","Custo Médio","CustoMed"]),
        "preco":     _pick_col(df, ["PreçoVenda","PrecoVenda","Preço Venda","Preco Venda","Preço","Valor"]),
        "estoque":   _pick_col(df, ["EstoqueAtual","Estoque","QtdEstoque","Quantidade"]),
        "est_min":   _pick_col(df, ["EstoqueMin","Estoque Min","Minimo","Mínimo"]),
    }

def _map_cols_compras(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "nome": _pick_col(df, ["Produto","Nome","Descrição","Descricao"]),
        "unid": _pick_col(df, ["Unidade","Unid"]),
        "forn": _pick_col(df, ["Fornecedor","FornecedorNome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "custo_unit": _pick_col(df, ["Custo Unitário","CustoUnit","Custo Unit","PrecoUnitario","Preço Unitário","CustoUnitario"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
    }

def _map_cols_mov(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "tipo": _pick_col(df, ["Tipo","Movimento","Mov"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "obs":  _pick_col(df, ["Obs","Observação","Observacoes","Observações"])
    }

def _map_cols_vendas(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"])
    }

# =============================================================================
# Carregar dados (abas do modelo)
# =============================================================================
ABA_PROD = "Produtos"
ABA_COMP = "Compras"
ABA_MOV  = "MovimentosEstoque"
ABA_VEND = "Vendas"

try:
    dfp = _load_df(ABA_PROD)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

dfc  = _safe_load(ABA_COMP)
dfm  = _safe_load(ABA_MOV)
dfv  = _safe_load(ABA_VEND)

COL  = _map_cols_produtos(dfp)
CMP  = _map_cols_compras(dfc) if not dfc.empty else {}
MOV  = _map_cols_mov(dfm) if not dfm.empty else {}
VEN  = _map_cols_vendas(dfv) if not dfv.empty else {}

MOV_HEADERS = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

# =============================================================================
# Parse numéricos
# =============================================================================
def _to_float(x):
    if x is None: return 0.0
    s = str(x).strip().replace("R$","").replace(" ", "")
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

def _fmt_int(v) -> str:
    try:
        return f"{int(round(float(v))):,}".replace(",", ".")
    except:
        return str(v)

# =============================================================================
# Cálculo de estoque atual (Compras + MovimentosEstoque + Vendas)
# =============================================================================
def _stock_balance(prod_id: str|None, nome: str) -> int:
    entr = 0.0; sai = 0.0
    # Compras (somam)
    if not dfc.empty and CMP:
        base = dfc.copy()
        if prod_id and CMP.get("id"):
            base = base[ base[CMP["id"]].astype(str) == str(prod_id) ]
        elif CMP.get("nome"):
            base = base[ base[CMP["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
        if not base.empty and CMP.get("qtd"):
            entr += base[CMP["qtd"]].apply(_to_float).sum()

    # MovimentosEstoque (entrada/saida/ajuste)
    if not dfm.empty and MOV:
        base = dfm.copy()
        if prod_id and MOV.get("id"):
            base = base[ base[MOV["id"]].astype(str) == str(prod_id) ]
        elif MOV.get("nome"):
            base = base[ base[MOV["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
        if not base.empty and MOV.get("tipo") and MOV.get("qtd"):
            tipos_ent = {"entrada","compra","ajuste+","entrada manual","in","b entrada"}
            tipos_sai = {"saida","venda","ajuste-","saída manual","out","b saída","b saida"}
            ent_m = base[ base[MOV["tipo"]].astype(str).str.lower().isin(tipos_ent) ][MOV["qtd"]].apply(_to_float).sum()
            sai_m = base[ base[MOV["tipo"]].astype(str).str.lower().isin(tipos_sai) ][MOV["qtd"]].apply(_to_float).sum()
            entr += (ent_m or 0.0); sai += (sai_m or 0.0)

    # Vendas (subtraem) — só se não houver Movimentos para vendas
    if not dfv.empty and VEN:
        base = dfv.copy()
        if prod_id and VEN.get("id"):
            base = base[ base[VEN["id"]].astype(str) == str(prod_id) ]
        elif VEN.get("nome"):
            base = base[ base[VEN["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
        if not base.empty and VEN.get("qtd"):
            # só considerar se não há lançamentos "venda" em Movimentos
            if dfm.empty or not MOV or MOV.get("tipo") is None:
                sai += base[VEN["qtd"]].apply(_to_float).sum()
            else:
                # há MOV; tenta detectar se existem tipos de venda no MOV para este produto; se não, usa Vendas como fallback
                check = dfm.copy()
                if prod_id and MOV.get("id"):
                    check = check[ check[MOV["id"]].astype(str) == str(prod_id) ]
                elif MOV.get("nome"):
                    check = check[ check[MOV["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
                has_mov_venda = False
                if not check.empty and MOV.get("tipo"):
                    has_mov_venda = check[MOV["tipo"]].astype(str).str.lower().isin({"venda","saida","saída manual","out","b saída","b saida"}).any()
                if not has_mov_venda:
                    sai += base[VEN["qtd"]].apply(_to_float).sum()

    try:
        return int(round(entr - sai, 0))
    except:
        return 0

# =============================================================================
# UI — Seleção e ajuste
# =============================================================================
col_id = COL["id"]; col_nome = COL["nome"]
if not col_id:
    st.error("Coluna de ID não encontrada na aba Produtos."); st.stop()

# Lista amigável
base_opts = dfp[[col_id] + ([col_nome] if col_nome else [])].astype(str).fillna("")
def _fmt_opt(r):
    if col_nome:
        return f"{r[col_id]} — {r[col_nome]}"
    return f"{r[col_id]}"
labels = ["(selecione)"] + base_opts.apply(_fmt_opt, axis=1).tolist()
escolha = st.selectbox("Produto", labels, index=0)

if escolha == "(selecione)":
    st.info("Selecione um produto para definir o estoque.")
    st.stop()

# Resolve ID e nome
pid = escolha.split(" — ")[0].strip()
try:
    nome_sel = dfp.loc[dfp[col_id].astype(str) == pid, col_nome].iloc[0] if col_nome else ""
except:
    nome_sel = ""

# Estoque atual
atual = float(_stock_balance(pid, nome_sel))
st.info(f"Estoque atual (calculado): **{_fmt_int(atual)}**")

# Formulário de ajuste
with st.form("form_ajuste"):
    c1, c2 = st.columns([1,1])
    with c1:
        novo_nivel = st.number_input("Definir estoque para", min_value=0, step=1, value=int(atual))
    with c2:
        motivo_default = "Contagem inicial" if int(atual) == 0 else "Contagem"
        motivo = st.text_input("Motivo", value=motivo_default)

    resp = st.text_input("Responsável", value="")
    obs  = st.text_area("Observações (opcional)", height=70, placeholder="Ex.: ajuste após contagem de prateleira")
    salvar = st.form_submit_button("💾 Salvar contagem", type="primary", use_container_width=True)

if salvar:
    delta = int(novo_nivel - atual)
    st.caption(f"Ajuste que será gravado: **{delta:+d}** (positivo entra / negativo sai)")
    if delta == 0:
        st.warning("Nada a ajustar — já está com essa quantidade."); st.stop()

    # Grava como MOVIMENTO de estoque (ajuste+ / ajuste-)
    ws_mov = _ensure_ws(ABA_MOV, ["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

    tipo_mov = "ajuste+" if delta > 0 else "ajuste-"
    agora = datetime.now()
    row_mov = {
        "Data": agora.strftime("%d/%m/%Y"),
        "IDProduto": pid,
        "Produto": nome_sel or "",
        "Tipo": tipo_mov,
        "Qtd": str(abs(delta)),
        "Obs": f"{motivo or '-'} | Resp: {resp or '-'}" + (f" | {obs.strip()}" if (obs or "").strip() else "")
    }
    _append_row(ws_mov, row_mov)

    # Feedback + cache
    _msg_ok(f"Contagem salva! Ajuste de {delta:+d} para {pid}.")
    st.session_state["_force_refresh"] = True

    # Telegram
    try:
        msg = (
            "📦 <b>Ajuste de Estoque</b>\n"
            f"🧾 Produto: <b>{html.escape(nome_sel or '-')}</b>\n"
            f"🔢 ID: <code>{html.escape(pid)}</code>\n"
            f"📅 {agora.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"📊 De: <b>{_fmt_int(atual)}</b> → Para: <b>{_fmt_int(novo_nivel)}</b>\n"
            f"➕➖ Ajuste: <b>{_fmt_int(delta)}</b> ({'entrada' if delta>0 else 'saída'})\n"
            f"📝 Motivo: {html.escape(motivo or '-')}\n"
            f"👤 Responsável: {html.escape(resp or '-')}\n"
            f"🗒️ Obs.: {html.escape((obs or '-').strip())}"
        )
        _tg_send(msg)
    except:
        pass

st.divider()
st.page_link("pages/01_produtos.py", label="↩️ Ir para Catálogo de Produtos", icon="📦")
st.page_link("pages/03_compras_entradas.py", label="🧾 Ir para Compras/Entradas", icon="🧾")
