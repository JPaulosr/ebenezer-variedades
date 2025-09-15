# pages/03_contagem_inicial.py — Contagem de estoque (definir nível)
# -*- coding: utf-8 -*-
import json, unicodedata, html
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # >>> NOVO: Telegram

st.set_page_config(page_title="Contagem de estoque", page_icon="📋", layout="wide")
st.title("📋 Contagem de estoque (definir nível)")

# =========================
# Helpers gerais
# =========================
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

# =========================
# Telegram
# =========================
def _send_telegram_message(text_html: str) -> tuple[bool, str]:
    """Envia mensagem para Telegram usando secrets TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID_ESTOQUE."""
    token = st.secrets.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_ESTOQUE", "").strip()
    if not token or not chat_id:
        return False, "Secrets TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID_ESTOQUE ausentes."

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, data=payload, timeout=15)
        if r.ok:
            return True, "Notificação enviada."
        return False, f"Falha HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"Erro de rede: {e}"

def _fmt_int(v) -> str:
    try:
        return f"{int(round(float(v))):,}".replace(",", ".")
    except:
        return str(v)

# =========================
# Carrega dados
# =========================
ABA_PROD, ABA_COMP, ABA_VEND, ABA_AJ = "Produtos", "Compras", "Vendas", "Ajustes"

try:
    dfp = carregar_aba(ABA_PROD)
except Exception:
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
col_a_id = _first_col(dfa, ["IDProduto","ID"])
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

# =========================
# UI
# =========================
dfp["_ID_join"] = dfp[col_id_prod].astype(str)
opts = dfp[[col_id_prod, col_nome]].astype(str).fillna("")
label = lambda r: f"{r[col_id_prod]} — {r[col_nome]}" if col_nome else r[col_id_prod]
lista = ["(selecione)"] + opts.apply(label, axis=1).tolist()

sel = st.selectbox("Produto", lista, index=0)
if sel != "(selecione)":
    pid = sel.split(" — ")[0]
    atual = float(estoque_calc.get(pid, 0.0))
    nome_prod = ""
    try:
        nome_prod = dfp.loc[dfp[col_id_prod].astype(str) == str(pid), col_nome].iloc[0] if col_nome else ""
    except:
        nome_prod = ""

    st.info(f"Estoque atual (calculado): **{atual:.0f}**")

    nova = st.number_input("Definir estoque para:", min_value=0, step=1, value=int(atual))

    # >>> NOVO: campos extras para o ajuste
    col1, col2 = st.columns(2)
    with col1:
        motivo_default = "Contagem inicial" if atual == 0 else "Contagem"
        motivo = st.text_input("Motivo", value=motivo_default)
    with col2:
        responsavel = st.text_input("Responsável", value="")

    obs = st.text_area("Observações (opcional)", value="", height=80)

    delta = int(nova - atual)
    st.caption(f"Ajuste que será gravado: **{delta:+d}** (positivo entra / negativo sai)")

    if st.button("Salvar contagem", type="primary", use_container_width=True):
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

            agora = datetime.now()
            data_str = agora.strftime("%d/%m/%Y")
            hora_str = agora.strftime("%H:%M:%S")

            nova_linha = {
                "Data": data_str,
                col_id_final: pid,
                "Qtd": str(delta),
                "Motivo": motivo or ( "Contagem inicial" if atual == 0 else "Contagem"),
                "Responsável": responsavel or "",
                "Obs": obs or ""
            }
            dfa2 = pd.concat([dfa2, pd.DataFrame([nova_linha])], ignore_index=True)

            # grava
            ws.clear()
            set_with_dataframe(ws, dfa2)

            # >>> auto-refresh nas outras páginas
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # >>> NOVO: Telegram
            # Monta mensagem HTML segura
            p_nome = html.escape(str(nome_prod or "-"))
            p_id   = html.escape(str(pid))
            p_mot  = html.escape(nova_linha["Motivo"])
            p_resp = html.escape(nova_linha["Responsável"] or "-")
            p_obs  = html.escape(nova_linha["Obs"] or "-")

            msg = (
                "📦 <b>Ajuste de Estoque</b>\n"
                f"🧾 Produto: <b>{p_nome}</b>\n"
                f"🔢 ID: <code>{p_id}</code>\n"
                f"📅 Data: {html.escape(data_str)} ({html.escape(hora_str)})\n"
                f"📊 De: <b>{_fmt_int(atual)}</b> → Para: <b>{_fmt_int(nova)}</b>\n"
                f"➕➖ Ajuste: <b>{_fmt_int(delta)}</b>\n"
                f"📝 Motivo: {p_mot}\n"
                f"👤 Responsável: {p_resp}\n"
                f"🗒️ Obs.: {p_obs}"
            )
            ok, info = _send_telegram_message(msg)
            if ok:
                st.success(f"Contagem salva e Telegram notificado! Ajuste de {delta:+d} para {pid}.")
            else:
                st.success(f"Contagem salva! Ajuste de {delta:+d} para {pid}.")
                st.warning(f"Telegram não enviado: {info}")
else:
    st.info("Selecione um produto para definir o estoque.")
