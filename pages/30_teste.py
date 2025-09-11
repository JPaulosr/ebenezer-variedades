# -*- coding: utf-8 -*-
# 12_Fiado.py — Fiado + Telegram (foto + card), por funcionário + cópia p/ JP
# - Lançar fiado (único e em lote)
# - Quitar por COMPETÊNCIA (por ID do combo ou por LINHA)
# - Notificações com FOTO e card HTML; Vinicius → canal / JPaulo → privado
# - Comissão só p/ elegíveis (Vinicius) — 50%
# - 💳 Maquininha: grava LÍQUIDO no campo Valor da BASE e preenche extras somente se usar_cartao=True
# - Fiado_Pagamentos salva TotalLiquido + TotalBruto + Taxa
# - 📗 Histórico de pagos (filtros + exportação)
# - Datas de REGISTRO extraídas do ID (L-YYYYMMDDHHMMSSmmm) — SEM hora
# - Em lote: campo "Valor" para serviço único; combos seguem valores padrão
# - 💝 CaixinhaDia: padrão único (MESMA LINHA DO ATENDIMENTO). Na quitação:
#     • Por ID: grava CaixinhaDia apenas na PRIMEIRA linha de cada ID selecionado
#     • Por Linha: grava CaixinhaDia apenas na PRIMEIRA linha selecionada
#   (não cria nova linha!)

import streamlit as st
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from datetime import date, datetime, timedelta
from io import BytesIO
import pytz
import unicodedata

# =============================
# CONFIG BÁSICA
# =============================
st.set_page_config(page_title="Fiado | Salão JP", page_icon="💳", layout="wide",
                   initial_sidebar_state="expanded")
st.title("💳 Controle de Fiado (combo por linhas + edição de valores)")

SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_BASE   = "Base de Dados"
ABA_LANC   = "Fiado_Lancamentos"
ABA_PAGT   = "Fiado_Pagamentos"
ABA_TAXAS  = "Cartao_Taxas"
STATUS_ABA = "clientes_status"

TZ = pytz.timezone("America/Sao_Paulo")
DATA_FMT = "%d/%m/%Y"

def today_local() -> date:
    """Data local respeitando o fuso definido em TZ (evita adiantar 1 dia)."""
    return datetime.now(TZ).date()

BASE_COLS_MIN = ["Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário","Fase","Tipo","Período"]
EXTRA_COLS    = ["StatusFiado","IDLancFiado","VencimentoFiado","DataPagamento"]
BASE_PAG_EXTRAS = [
    "ValorBrutoRecebido", "ValorLiquidoRecebido",
    "TaxaCartaoValor", "TaxaCartaoPct",
    "FormaPagDetalhe", "PagamentoID"
]
# Caixinha padrão único (igual 11_Adicionar_Atendimento)
CAIXA_COLS = ["CaixinhaDia"]

BASE_COLS_ALL = BASE_COLS_MIN + EXTRA_COLS + BASE_PAG_EXTRAS + CAIXA_COLS

VALORES_PADRAO = {
    "Corte": 25.0, "Pezinho": 7.0, "Barba": 15.0, "Sobrancelha": 7.0,
    "Luzes": 45.0, "Pintura": 35.0, "Alisamento": 40.0, "Gel": 10.0, "Pomada": 15.0
}

COMISSAO_FUNCIONARIOS = {"vinicius"}   # case-insensitive
COMISSAO_PERC_PADRAO = 0.50

TAXAS_COLS = ["IDPagamento","Cliente","DataPag","Bandeira","Tipo","Parcelas","Bruto","Liquido","TaxaValor","TaxaPct","IDLancs"]
PAGT_COLS  = ["IDPagamento","IDLancs","DataPagamento","Cliente","Forma","TotalLiquido","Obs","TotalBruto","TaxaValor","TaxaPct"]

# =============================
# TELEGRAM
# =============================
TELEGRAM_TOKEN_CONST           = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO_CONST  = "493747253"
TELEGRAM_CHAT_ID_VINICIUS_CONST= "-1002953102982"  # canal do Vinícius

def _get_secret(name: str, default: str | None = None) -> str | None:
    try:
        val = st.secrets.get(name)
        val = (val or "").strip()
        if val:
            return val
    except Exception:
        pass
    return (default or "").strip() or None

def _get_token() -> str | None:
    return _get_secret("TELEGRAM_TOKEN", TELEGRAM_TOKEN_CONST)

def _get_chat_id_jp() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_JPAULO", TELEGRAM_CHAT_ID_JPAULO_CONST)

def _get_chat_id_vini() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_VINICIUS", TELEGRAM_CHAT_ID_VINICIUS_CONST)

def _check_tg_ready(token: str | None, chat_id: str | None) -> bool:
    return bool((token or "").strip() and (chat_id or "").strip())

def _chat_id_por_func(funcionario: str) -> str | None:
    if str(funcionario).strip() == "Vinicius":
        return _get_chat_id_vini()
    return _get_chat_id_jp()

def tg_send(text: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        js = r.json()
        return bool(r.ok and js.get("ok"))
    except Exception:
        return False

def tg_send_photo(photo_url: str, caption: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        data = {"chat_id": chat, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=data, timeout=30)
        js = r.json()
        if r.ok and js.get("ok"):
            return True
        return tg_send(caption, chat_id=chat)
    except Exception:
        return tg_send(caption, chat_id=chat)

# =============================
# FOTOS (clientes_status)
# =============================
FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

@st.cache_data(show_spinner=False)
def carregar_fotos_mapa():
    try:
        sh = conectar_sheets()
        if STATUS_ABA not in [w.title for w in sh.worksheets()]:
            return {}
        ws = sh.worksheet(STATUS_ABA)
        df = get_as_dataframe(ws).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
        cols_lower = {c.lower(): c for c in df.columns}
        foto_col = next((cols_lower[c] for c in FOTO_COL_CANDIDATES if c in cols_lower), None)
        cli_col  = next((cols_lower[c] for c in ["cliente","nome","nome_cliente"] if c in cols_lower), None)
        if not (foto_col and cli_col):
            return {}
        tmp = df[[cli_col, foto_col]].copy()
        tmp.columns = ["Cliente", "Foto"]
        tmp["k"] = tmp["Cliente"].astype(str).map(_norm)
        return {r["k"]: str(r["Foto"]).strip()
                for _, r in tmp.iterrows() if str(r["Foto"]).strip()}
    except Exception:
        return {}

def show_foto_cliente(cliente: str):
    try:
        k = _norm(cliente or "")
        url = FOTOS.get(k)
        if url:
            st.image(url, width=160, caption=cliente)
    except Exception:
        pass

# =============================
# UTILS
# =============================
def proxima_terca(d: date) -> date:
    wd = d.weekday()
    delta = (1 - wd) % 7
    return d + timedelta(days=delta)

def _fmt_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_pct(p: float) -> str:
    try:
        return f"{p:.2f}%"
    except Exception:
        return "-"

def _norm_key(s: str) -> str:
    return unicodedata.normalize("NFKC", str(s).strip()).casefold()

def col_map(ws):
    headers = ws.row_values(1)
    cmap = {}
    for i, h in enumerate(headers):
        k = _norm_key(h)
        if k and k not in cmap:
            cmap[k] = i + 1
    return cmap

def ensure_headers(ws, desired_headers):
    """Garante headers sem duplicação, comparando por nome normalizado."""
    import unicodedata
    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKC", str(s or "")).strip()
        return s.casefold()

    headers = ws.row_values(1)
    if not headers:
        ws.append_row(desired_headers)
        return {h: i+1 for i, h in enumerate(desired_headers)}

    # normaliza existentes e remove duplicatas mantendo o 1º
    seen = set()
    fixed = []
    for h in headers:
        k = _norm(h)
        if k in seen:
            continue
        seen.add(k)
        fixed.append(h.strip())

    # se houve mudança (duplicatas removidas), reescreve a linha 1 “limpa”
    if fixed != headers:
        ws.update('A1', [fixed])

    # adiciona apenas os que realmente faltam (por normalização)
    existing_norm = {_norm(h) for h in fixed}
    missing = [h for h in desired_headers if _norm(h) not in existing_norm]
    if missing:
        ws.update('A1', [fixed + missing])

    headers_final = ws.row_values(1)
    return {h: i+1 for i, h in enumerate(headers_final)}

def append_rows_generic(ws, dicts, default_headers=None):
    headers = ws.row_values(1)
    if not headers:
        headers = default_headers or sorted({k for d in dicts for k in d.keys()})
        ws.append_row(headers)
    hdr_norm = [_norm_key(h) for h in headers]
    rows = []
    for d in dicts:
        d_norm = {_norm_key(k): v for k, v in d.items()}
        rows.append([d_norm.get(hn, "") for hn in hdr_norm])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

def contains_cartao(s: str) -> bool:
    MAQ = {
        "cart", "cartao", "cartão",
        "credito", "crédito", "debito", "débito",
        "maquina", "maquininha", "maquineta", "pos",
        "pagseguro", "mercadopago", "mercado pago",
        "sumup", "stone", "cielo", "rede", "getnet", "safra",
        "visa", "master", "elo", "hiper", "amex",
        "nubank", "nubank cnpj"
    }
    x = unicodedata.normalize("NFKD", (s or "")).encode("ascii", "ignore").decode("ascii")
    x = x.lower().replace(" ", "")
    return any(k in x for k in MAQ)

def is_nao_cartao(conta: str) -> bool:
    s = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower()
    tokens = {"pix", "dinheiro", "carteira", "cash", "especie", "espécie",
              "transfer", "transferencia", "transferência", "ted", "doc"}
    return any(t in s for t in tokens)

def default_card_flag(conta: str) -> bool:
    s = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower().replace(" ", "")
    if "nubankcnpj" in s:
        return False
    if is_nao_cartao(conta):
        return False
    return contains_cartao(conta)

def servicos_compactos_por_ids_parcial(df_rows: pd.DataFrame) -> str:
    if df_rows.empty:
        return "-"
    partes = []
    for _, grp in df_rows.groupby("IDLancFiado"):
        servs = sorted(set(grp["Serviço"].dropna().astype(str).str.strip().tolist()))
        partes.append("+".join(servs) if servs else "-")
    vistos, out = [], []
    for p in partes:
        if p and p not in vistos:
            vistos.append(p); out.append(p)
    return " | ".join(out) if out else "-"

def historico_cliente_por_ano(df_base: pd.DataFrame, cliente: str) -> dict[int, float]:
    if df_base is None or df_base.empty or not cliente:
        return {}
    df = df_base.copy()
    df["__dt"] = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["__valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df = df[(df["Cliente"].astype(str).str.strip() == str(cliente).strip()) & df["__dt"].notna()]
    if df.empty:
        return {}
    grp = df.groupby(df["__dt"].dt.year)["__valor"].sum().to_dict()
    return {int(ano): float(round(v, 2)) for ano, v in grp.items()}

def breakdown_por_servico_no_ano(df_base: pd.DataFrame, cliente: str, ano: int, max_itens: int = 8):
    if df_base is None or df_base.empty or not cliente or not ano:
        return pd.DataFrame(columns=["Serviço","Qtd","Total"]), 0, 0.0, 0, 0.0
    df = df_base.copy()
    df["__dt"] = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["__valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df = df[(df["Cliente"].astype(str).str.strip() == str(cliente).strip()) & (df["__dt"].dt.year == ano)]
    if df.empty:
        return pd.DataFrame(columns=["Serviço","Qtd","Total"]), 0, 0.0, 0, 0.0
    agg = (df.groupby("Serviço", dropna=True)
             .agg(Qtd=("Serviço","count"), Total=("__valor","sum"))
             .reset_index()).sort_values("Total", ascending=False)
    total_qtd = int(agg["Qtd"].sum())
    total_val = float(agg["Total"].sum())
    top = agg.head(max_itens).copy()
    outros = agg.iloc[max_itens:] if len(agg) > max_itens else pd.DataFrame(columns=agg.columns)
    outros_qtd = int(outros["Qtd"].sum()) if not outros.empty else 0
    outros_val = float(outros["Total"].sum()) if not outros.empty else 0.0
    top["Qtd"] = top["Qtd"].astype(int)
    top["Total"] = top["Total"].astype(float).round(2)
    return top, total_qtd, total_val, outros_qtd, outros_val

def format_extras_numeric(ws):
    cmap = col_map(ws)
    def fmt(name, ntype, pattern):
        col = cmap.get(_norm_key(name))
        if not col:
            return
        a1_from = rowcol_to_a1(2, col)
        a1_to   = rowcol_to_a1(50000, col)
        try:
            ws.format(f"{a1_from}:{a1_to}", {"numberFormat": {"type": ntype, "pattern": pattern}})
        except Exception:
            pass
    fmt("ValorBrutoRecebido",   "NUMBER",  "0.00")
    fmt("ValorLiquidoRecebido", "NUMBER",  "0.00")
    fmt("TaxaCartaoValor",      "NUMBER",  "0.00")
    fmt("TaxaCartaoPct",        "PERCENT", "0.00%")
    fmt("CaixinhaDia",          "NUMBER",  "0.00")  # <- novo

# ---------- NOVO: datas/periodo de REGISTRO a partir do ID ----------
def _so_digitos(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())

def data_reg_do_id(idl: str):
    """Extrai a DATA (date) do IDLancFiado (L-YYYYMMDDHHMMSSmmm)."""
    try:
        digs = _so_digitos(idl)
        if len(digs) >= 8:
            return datetime.strptime(digs[:8], "%Y%m%d").date()
    except Exception:
        pass
    return None

def periodo_do_id(df: pd.DataFrame, idl: str) -> str:
    """Se todas as linhas do ID tiverem o mesmo Período, retorna ele; senão, ''. """
    try:
        vals = (df.loc[df["IDLancFiado"] == idl, "Período"]
                  .dropna().astype(str).str.strip())
        vals = [v for v in vals if v]
        uniq = sorted(set(vals))
        return uniq[0] if len(uniq) == 1 else ""
    except Exception:
        return ""

# =============================
# SHEETS OPS
# =============================
def garantir_aba(ss, nome, cols):
    try:
        ws = ss.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=nome, rows=200, cols=max(10, len(cols)))
        ws.append_row(cols)
        return ws
    existing = ws.row_values(1)
    if not existing:
        ws.append_row(cols)
    return ws

def read_base_raw(ss):
    ws = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
    ensure_headers(ws, BASE_COLS_ALL)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    for c in BASE_COLS_ALL:
        if c not in df.columns:
            df[c] = ""
    df = df[[*BASE_COLS_ALL, *[c for c in df.columns if c not in BASE_COLS_ALL]]]
    return df.fillna(""), ws

def append_rows_base(ws, novas_dicts):
    headers = ws.row_values(1)
    if not headers:
        headers = BASE_COLS_ALL
        ws.append_row(headers)
    hdr_norm = [_norm_key(h) for h in headers]
    rows = []
    for d in novas_dicts:
        d_norm = {_norm_key(k): v for k, v in d.items()}
        rows.append([d_norm.get(hn, "") for hn in hdr_norm])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

@st.cache_data
def carregar_listas():
    ss = conectar_sheets()
    ws_base = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
    ensure_headers(ws_base, BASE_COLS_ALL)

    df_list = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).fillna("")
    df_list.columns = [str(c).strip() for c in df_list.columns]
    df_list = df_list.loc[:, ~pd.Index(df_list.columns).duplicated(keep="first")]

    clientes = sorted([c for c in df_list.get("Cliente", "").astype(str).str.strip().unique() if c])
    combos  = sorted([c for c in df_list.get("Combo", "").astype(str).str.strip().unique() if c])
    servs   = sorted([s for s in df_list.get("Serviço", "").astype(str).str.strip().unique() if s])

    contas_raw = [c for c in df_list.get("Conta", "").astype(str).str.strip().unique() if c]
    base_contas = sorted([c for c in contas_raw if c.lower() != "fiado"])
    if "Nubank CNPJ" not in base_contas:
        base_contas.append("Nubank CNPJ")

    return clientes, combos, servs, base_contas

def gerar_id(prefixo):
    return f"{prefixo}-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def parse_combo(combo_str):
    if not combo_str:
        return []
    partes = [p.strip() for p in str(combo_str).split("+") if p.strip()]
    ajustadas = []
    for p in partes:
        hit = next((k for k in VALORES_PADRAO.keys() if k.lower() == p.lower()), p)
        ajustadas.append(hit)
    return ajustadas

def ultima_forma_pagto_cliente(df_base, cliente):
    if df_base.empty or not cliente:
        return None
    df = df_base[(df_base["Cliente"] == cliente) & df_base["Conta"].str.lower() != "fiado"].copy()
    if df.empty:
        return None
    try:
        df["__d"] = pd.to_datetime(df["Data"], format=DATA_FMT, errors="coerce")
        df = df.sort_values("__d", ascending=False)
    except Exception:
        pass
    return str(df.iloc[0]["Conta"]) if not df.empty else None

# ===== Caches
clientes, combos_exist, servs_exist, base_contas = carregar_listas()
FOTOS = carregar_fotos_mapa()

# =============================
# SIDEBAR
# =============================
st.sidebar.header("Ações")
acao = st.sidebar.radio(
    "Escolha:",
    ["➕ Lançar fiado", "💰 Registrar pagamento", "📋 Em aberto & exportação", "📗 Pagos (histórico)"]
)

# =============================
# FLUXOS
# =============================

# ---------- 1) Lançar fiado ----------
if acao == "➕ Lançar fiado":
    st.subheader("➕ Lançar fiado — cria UMA linha por serviço na Base (Conta='Fiado', StatusFiado='Em aberto')")

    tab_uni, tab_lote = st.tabs(["🧍 Lançamento único", "🗂️ Lançamento em lote"])

    # --- Único ---
    with tab_uni:
        c1, c2 = st.columns(2)
        with c1:
            cliente = st.selectbox("Cliente", options=[""] + clientes, index=0, key="fiado_cli_uni")
            if not cliente:
                cliente = st.text_input("Ou digite o nome do cliente", "", key="fiado_cli_txt_uni")
            if cliente:
                show_foto_cliente(cliente)
            combo_str = st.selectbox("Combo (use 'corte+barba')", [""] + combos_exist, key="fiado_combo_uni")
            servico_unico = st.selectbox("Ou selecione um serviço (se não usar combo)", [""] + servs_exist, key="fiado_serv_uni")
            funcionario = st.selectbox("Funcionário", ["JPaulo", "Vinicius"], index=0, key="fiado_func_uni")

        with c2:
            data_atend = st.date_input("Data do atendimento", value=today_local(), key="fiado_data_uni")
            venc = st.date_input("Vencimento (opcional)", value=today_local(), key="fiado_venc_uni")
            fase = st.text_input("Fase", value="Dono + funcionário", key="fiado_fase_uni")
            tipo = st.selectbox("Tipo", ["Serviço", "Produto"], index=0, key="fiado_tipo_uni")
            periodo = st.selectbox("Período (opcional)", ["", "Manhã", "Tarde", "Noite"], index=0, key="fiado_periodo_uni")

        servicos = parse_combo(combo_str) if combo_str else ([servico_unico] if servico_unico else [])
        valores_custom = {}
        if servicos:
            st.markdown("#### 💰 Edite os valores antes de salvar")
            for s in servicos:
                padrao = VALORES_PADRAO.get(s, 0.0)
                valores_custom[s] = st.number_input(
                    f"{s} (padrão: R$ {padrao:.2f})", value=float(padrao),
                    step=1.0, format="%.2f", key=f"valor_{s}_uni"
                )

        if st.button("Salvar fiado (único)", use_container_width=True, key="btn_salvar_uni"):
            if not cliente:
                st.error("Informe o cliente.")
            elif not servicos:
                st.error("Informe combo ou um serviço.")
            else:
                idl = gerar_id("L")
                data_str = data_atend.strftime(DATA_FMT)
                venc_str = venc.strftime(DATA_FMT) if venc else ""
                novas = []
                for s in servicos:
                    valor_item = float(valores_custom.get(s, VALORES_PADRAO.get(s, 0.0)))
                    novas.append({
                        "Data": data_str, "Serviço": s, "Valor": valor_item, "Conta": "Fiado",
                        "Cliente": cliente, "Combo": combo_str if combo_str else "", "Funcionário": funcionario,
                        "Fase": fase, "Tipo": tipo, "Período": periodo,
                        "StatusFiado": "Em aberto", "IDLancFiado": idl, "VencimentoFiado": venc_str,
                        "DataPagamento": "",
                        "ValorBrutoRecebido":"", "ValorLiquidoRecebido":"", "TaxaCartaoValor":"", "TaxaCartaoPct":"",
                        "FormaPagDetalhe":"", "PagamentoID":"", "CaixinhaDia":""
                    })
                ss = conectar_sheets()
                ws_base = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
                ensure_headers(ws_base, BASE_COLS_ALL)
                append_rows_base(ws_base, novas)

                total = float(pd.to_numeric(pd.DataFrame(novas)["Valor"], errors="coerce").fillna(0).sum())
                ws_l = garantir_aba(ss, ABA_LANC, ["IDLanc","Data","Cliente","Combo","Servicos","Total","Venc","Func","Fase","Tipo","Periodo"])
                append_rows_generic(ws_l, [{
                    "IDLanc": idl, "Data": data_str, "Cliente": cliente, "Combo": combo_str,
                    "Servicos": "+".join(servicos), "Total": total, "Venc": venc_str, "Func": funcionario,
                    "Fase": fase, "Tipo": tipo, "Periodo": periodo
                }])

                st.success(f"Fiado criado para **{cliente}** — ID: {idl}. Geradas {len(novas)} linhas na Base.")
                st.cache_data.clear()

                try:
                    total_fmt = _fmt_brl(total)
                    servicos_txt = combo_str.strip() if (combo_str and combo_str.strip()) else ("+".join(servicos) if servicos else "-")
                    msg_html = (
                        "🧾 <b>Novo fiado criado</b>\n"
                        f"👤 Cliente: <b>{cliente}</b>\n"
                        f"🧰 Serviço(s): <b>{servicos_txt}</b>\n"
                        f"💵 Total: <b>{total_fmt}</b>\n"
                        f"📅 Atendimento: {data_str}\n"
                        f"⏳ Vencimento: {venc_str or '-'}\n"
                        f"🆔 ID: <code>{idl}</code>"
                    )
                    chat_dest = _chat_id_por_func(funcionario)
                    foto = FOTOS.get(_norm(cliente))
                    if foto: tg_send_photo(foto, msg_html, chat_id=chat_dest)
                    else:    tg_send(msg_html, chat_id=chat_dest)
                except Exception:
                    pass

    # --- Lote ---
    with tab_lote:
        st.caption("💡 Preencha várias linhas e clique em **Salvar fiados (lote)**. "
                   "Se for serviço único, edite o campo Valor. Para combos o valor segue a tabela padrão por serviço.")
        num_linhas = st.number_input("Quantidade de linhas", min_value=1, max_value=200, value=5, step=1, key="fiado_qtd_lote")

        opcoes_combo_serv_brutas = [*sorted(combos_exist), *sorted(servs_exist)]
        opcoes_combo_serv = list(dict.fromkeys(["Corte", *opcoes_combo_serv_brutas]))
        valor_corte = float(VALORES_PADRAO.get("Corte", 0.0))

        df_modelo = pd.DataFrame({
            "Cliente": ["" for _ in range(num_linhas)],
            "Funcionário": ["Vinicius" for _ in range(num_linhas)],
            "Data": [today_local() for _ in range(num_linhas)],
            "Vencimento": [today_local() for _ in range(num_linhas)],
            "Fase": ["Dono + funcionário" for _ in range(num_linhas)],
            "Tipo": ["Serviço" for _ in range(num_linhas)],
            "Período": ["" for _ in range(num_linhas)],
            "Combo_ou_Serviço": ["Corte" for _ in range(num_linhas)],
            "Valor": [valor_corte for _ in range(num_linhas)],
        })

        col_editor, col_foto = st.columns([3,1])
        with col_editor:
            edited = st.data_editor(
                df_modelo, num_rows="dynamic", use_container_width=True, key="fiado_editor_lote",
                column_config={
                    "Cliente": st.column_config.SelectboxColumn(
                        options=[""] + clientes,
                        help="Escolha um cliente já cadastrado"
                    ),
                    "Funcionário": st.column_config.SelectboxColumn(options=["JPaulo", "Vinicius"]),
                    "Data": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Vencimento": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "Fase": st.column_config.TextColumn(),
                    "Tipo": st.column_config.SelectboxColumn(options=["Serviço", "Produto"]),
                    "Período": st.column_config.SelectboxColumn(options=["", "Manhã", "Tarde", "Noite"]),
                    "Combo_ou_Serviço": st.column_config.SelectboxColumn(
                        options=[""] + opcoes_combo_serv,
                        help="Escolha um combo existente (ex.: corte+barba) OU um serviço único"
                    ),
                    "Valor": st.column_config.NumberColumn(
                        help="Para serviço único, edite o valor. Em combos é ignorado.",
                        step=1.0, format="%.2f", min_value=0.0
                    ),
                },
            )
        with col_foto:
            try:
                primeira_linha_cli = next((str(x).strip() for x in edited["Cliente"].tolist() if str(x).strip()), "")
                if primeira_linha_cli:
                    st.caption("Foto (primeira linha com cliente preenchido):")
                    show_foto_cliente(primeira_linha_cli)
            except Exception:
                pass

        try:
            for idx in edited.index:
                nome = str(edited.at[idx, "Combo_ou_Serviço"]).strip() or "Corte"
                edited.at[idx, "Combo_ou_Serviço"] = nome
                if "+" not in nome and nome in VALORES_PADRAO:
                    val_atual = float(edited.at[idx, "Valor"] or 0.0)
                    if val_atual <= 0.0:
                        edited.at[idx, "Valor"] = float(VALORES_PADRAO.get(nome, 0.0))
        except Exception:
            pass

        if st.button("Salvar fiados (lote)", use_container_width=True, key="btn_salvar_lote"):
            linhas_validas = edited.dropna(how="all")
            linhas_validas = linhas_validas[linhas_validas["Cliente"].astype(str).str.strip() != ""]
            if linhas_validas.empty:
                st.error("Preencha pelo menos uma linha com Cliente e Combo_ou_Serviço.")
            else:
                clientes_ok = {str(c).strip() for c in clientes}
                invalidos = sorted(
                    {
                        str(c).strip()
                        for c in linhas_validas["Cliente"].tolist()
                        if str(c).strip() not in clientes_ok
                    }
                )
                if invalidos:
                    st.error("Há cliente(s) não cadastrados no lote: " + ", ".join(invalidos) + ". Corrija no seletor.")
                    st.stop()

                ss = conectar_sheets()
                ws_base = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
                ensure_headers(ws_base, BASE_COLS_ALL)
                ws_l = garantir_aba(ss, ABA_LANC, ["IDLanc","Data","Cliente","Combo","Servicos","Total","Venc","Func","Fase","Tipo","Periodo"])

                total_registros = 0
                for _, r in linhas_validas.iterrows():
                    cliente_i = str(r["Cliente"]).strip()
                    funcionario_i = str(r["Funcionário"]).strip() or "Vinicius"
                    data_i = r["Data"] if isinstance(r["Data"], date) else today_local()
                    venc_i = r["Vencimento"] if isinstance(r["Vencimento"], date) else today_local()
                    fase_i = str(r["Fase"]).strip() or "Dono + funcionário"
                    tipo_i = str(r["Tipo"]).strip() or "Serviço"
                    periodo_i = str(r["Período"]).strip()
                    escolha_i = str(r["Combo_ou_Serviço"]).strip() or "Corte"
                    valor_edit = float(r.get("Valor", 0.0) or 0.0)

                    if "+" in escolha_i:
                        servicos_i = parse_combo(escolha_i) or []
                        combo_str = escolha_i
                    else:
                        servicos_i = [escolha_i]
                        combo_str = ""

                    if not servicos_i:
                        continue

                    idl = gerar_id("L")
                    data_str = data_i.strftime(DATA_FMT)
                    venc_str = venc_i.strftime(DATA_FMT) if venc_i else ""

                    novas = []
                    for s in servicos_i:
                        valor_item = float(VALORES_PADRAO.get(s, 0.0)) if combo_str else float(valor_edit if valor_edit > 0 else VALORES_PADRAO.get(s, 0.0))
                        novas.append({
                            "Data": data_str, "Serviço": s, "Valor": valor_item, "Conta": "Fiado",
                            "Cliente": cliente_i, "Combo": (combo_str if combo_str else ""),
                            "Funcionário": funcionario_i, "Fase": fase_i, "Tipo": tipo_i, "Período": periodo_i,
                            "StatusFiado": "Em aberto", "IDLancFiado": idl, "VencimentoFiado": venc_str,
                            "DataPagamento": "",
                            "ValorBrutoRecebido":"", "ValorLiquidoRecebido":"", "TaxaCartaoValor":"", "TaxaCartaoPct":"",
                            "FormaPagDetalhe":"", "PagamentoID":"", "CaixinhaDia":""
                        })

                    append_rows_base(ws_base, novas)
                    total = float(pd.to_numeric(pd.DataFrame(novas)["Valor"], errors="coerce").fillna(0).sum())
                    append_rows_generic(ws_l, [{
                        "IDLanc": idl, "Data": data_str, "Cliente": cliente_i,
                        "Combo": (combo_str if combo_str else ""),
                        "Servicos": "+".join(servicos_i), "Total": total, "Venc": venc_str, "Func": funcionario_i,
                        "Fase": fase_i, "Tipo": tipo_i, "Periodo": periodo_i
                    }])

                    total_registros += 1

                    try:
                        total_fmt = _fmt_brl(total)
                        servicos_txt = combo_str if combo_str else "+".join(servicos_i)
                        msg_html = (
                            "🧾 <b>Novo fiado criado</b>\n"
                            f"👤 Cliente: <b>{cliente_i}</b>\n"
                            f"🧰 Serviço(s): <b>{servicos_txt}</b>\n"
                            f"💵 Total: <b>{total_fmt}</b>\n"
                            f"📅 Atendimento: {data_str}\n"
                            f"⏳ Vencimento: {venc_str or '-'}\n"
                            f"🆔 ID: <code>{idl}</code>"
                        )
                        chat_dest = _chat_id_por_func(funcionario_i)
                        foto = FOTOS.get(_norm(cliente_i))
                        if foto: tg_send_photo(foto, msg_html, chat_id=chat_dest)
                        else:    tg_send(msg_html, chat_id=chat_dest)
                    except Exception:
                        pass

                st.success(f"Lote concluído! {total_registros} fiado(s) criados.")
                st.cache_data.clear()

# ---------- 2) Registrar pagamento ----------
elif acao == "💰 Registrar pagamento":
    st.subheader("💰 Registrar pagamento — escolha o cliente e depois o(s) fiado(s) em aberto")

    ss = conectar_sheets()
    df_base_full, ws_base = read_base_raw(ss)

    df_abertos = df_base_full[df_base_full.get("StatusFiado", "") == "Em aberto"].copy()
    clientes_abertos = sorted(df_abertos["Cliente"].dropna().astype(str).str.strip().unique().tolist())

    colc1, colc2 = st.columns([1, 1])
    with colc1:
        cliente_sel = st.selectbox("Cliente com fiado em aberto", options=[""] + clientes_abertos, index=0)
        if cliente_sel:
            show_foto_cliente(cliente_sel)

    ultima = ultima_forma_pagto_cliente(df_base_full, cliente_sel) if cliente_sel else None
    lista_contas_default = ["Pix","Dinheiro","Cartão","Transferência","Pagseguro","Mercado Pago","Nubank CNPJ",
                            "SumUp","Cielo","Stone","Getnet","Outro","Nubank"]
    lista_contas = sorted(set(base_contas + lista_contas_default), key=lambda s: s.lower())
    default_idx = lista_contas.index(ultima) if (ultima in lista_contas) else 0
    with colc2:
        forma_pag = st.selectbox("Forma de pagamento (quitação)", options=lista_contas, index=default_idx)

    force_off = is_nao_cartao(forma_pag)
    usar_cartao = st.checkbox(
        "Tratar como cartão (com taxa)?",
        value=(False if force_off else default_card_flag(forma_pag)),
        disabled=force_off,
        help=("Desabilitado para PIX/Dinheiro/Transferência." if force_off else "Use quando passar no POS/NFC.")
    )

    modo_sel = st.radio("Modo de seleção de quitação", ["Por ID (combo inteiro)", "Por linha (serviço)"], index=0, horizontal=True)

    ids_opcoes, id_selecionados = [], []
    linhas_label_map, linhas_indices_sel = {}, []
    grupo_cli = pd.DataFrame()

    if cliente_sel:
        grupo_cli = df_abertos[df_abertos["Cliente"].astype(str).str.strip() == str(cliente_sel).strip()].copy()

        if modo_sel.startswith("Por ID"):
            grupo_cli["Data"]  = pd.to_datetime(grupo_cli["Data"], format=DATA_FMT, errors="coerce").dt.strftime(DATA_FMT)
            grupo_cli["Valor"] = pd.to_numeric(grupo_cli["Valor"], errors="coerce").fillna(0)

            def atraso_max(idval):
                v = grupo_cli.loc[grupo_cli["IDLancFiado"] == idval, "VencimentoFiado"].dropna().astype(str)
                try:
                    vdt = pd.to_datetime(v.iloc[0], format=DATA_FMT, errors="coerce").date() if not v.empty else None
                except Exception:
                    vdt = None
                if vdt:
                    d = (today_local() - vdt).days
                    return d if d > 0 else 0
                return 0

            resumo_ids = (
                grupo_cli.groupby("IDLancFiado", as_index=False)
                .agg(Data=("Data","min"), ValorTotal=("Valor","sum"), Qtde=("Serviço","count"), Combo=("Combo","first"))
            )
            for _, r in resumo_ids.iterrows():
                atraso = atraso_max(r["IDLancFiado"])
                badge = "Em dia" if atraso <= 0 else f"{int(atraso)}d atraso"

                dt_reg = data_reg_do_id(r["IDLancFiado"])
                periodo_id = periodo_do_id(grupo_cli, r["IDLancFiado"])

                partes = [r["IDLancFiado"]]
                if dt_reg:
                    partes.append(f"reg: {dt_reg.strftime(DATA_FMT)}")
                if periodo_id:
                    partes.append(periodo_id)

                rotulo = " • ".join(partes) + f" • {int(r['Qtde'])} serv. • R$ {r['ValorTotal']:.2f} • {badge}"
                if pd.notna(r["Combo"]) and str(r["Combo"]).strip():
                    rotulo += f" • {r['Combo']}"
                ids_opcoes.append((r["IDLancFiado"], rotulo))

            ids_valores = [i[0] for i in ids_opcoes]
            labels_id = {i: l for i, l in ids_opcoes}
            select_all_ids = st.checkbox("Selecionar todos os fiados deste cliente", value=False, disabled=not bool(ids_valores))
            id_selecionados = st.multiselect(
                "Selecione 1 ou mais fiados do cliente",
                options=ids_valores,
                default=(ids_valores if select_all_ids else []),
                format_func=lambda x: labels_id.get(x, x),
            )
        else:
            linhas_cli = grupo_cli.copy()
            linhas_cli["IdxBase"] = linhas_cli.index
            linhas_cli["DataFmt"] = pd.to_datetime(linhas_cli["Data"], format=DATA_FMT, errors="coerce").dt.strftime(DATA_FMT)
            linhas_cli["ValorNum"] = pd.to_numeric(linhas_cli["Valor"], errors="coerce").fillna(0.0)
            for _, r in linhas_cli.iterrows():
                lbl = f"{r['IDLancFiado']} • {r['DataFmt'] or '-'} • {r['Serviço']} • R$ {r['ValorNum']:.2f} • {r['Funcionário']}"
                linhas_label_map[int(r["IdxBase"])] = lbl
            linhas_todas = list(linhas_label_map.keys())
            select_all_linhas = st.checkbox("Selecionar todas as linhas em aberto deste cliente", value=False, disabled=not bool(linhas_todas))
            linhas_indices_sel = st.multiselect(
                "Selecione linhas específicas do cliente (por serviço)",
                options=linhas_todas,
                default=(linhas_todas if select_all_linhas else []),
                format_func=lambda i: linhas_label_map.get(i, str(i)),
            )

    # -----------------------------------------------
    # AJUSTE: Data do pagamento SEMPRE hoje (default)
    # Mantemos a legenda "Registro do ID: ..." só como referência,
    # sem alterar o valor do input.
    # -----------------------------------------------
    data_pag_default = today_local()
    registro_caption = None
    if modo_sel.startswith("Por ID") and len(id_selecionados) == 1:
        d = data_reg_do_id(id_selecionados[0])
        if d:
            registro_caption = d.strftime(DATA_FMT)
    # -----------------------------------------------

    cold1, cold2 = st.columns(2)
    with cold1:
        data_pag = st.date_input("Data do pagamento", value=data_pag_default)
        if registro_caption:
            st.caption(f"Registro do ID: {registro_caption}")
    with cold2:
        obs = st.text_input("Observação (opcional)", "", key="obs")

    # 💝 Caixinha (opcional) — MESMA LINHA do(s) atendimento(s)
    caixinha_dia_val = st.number_input(
        "💝 Caixinha do dia (opcional) — será gravada na mesma linha do atendimento",
        min_value=0.0, step=1.0, format="%.2f", value=0.00
    )

    # Preview / totais
    total_sel = 0.0
    valor_liquido_cartao = None
    bandeira_cartao = ""
    tipo_cartao = "Crédito"
    parcelas_cartao = 1
    taxa_valor_est = 0.0
    taxa_pct_est = 0.0
    subset_preview = pd.DataFrame()

    if cliente_sel:
        if modo_sel.startswith("Por ID"):
            subset_preview = df_abertos[df_abertos["IDLancFiado"].isin(id_selecionados)].copy()
        else:
            subset_preview = df_abertos[df_abertos.index.isin(linhas_indices_sel)].copy()

    if not subset_preview.empty:
        subset_preview["Valor"] = pd.to_numeric(subset_preview["Valor"], errors="coerce").fillna(0)
        total_sel = float(subset_preview["Valor"].sum())

        st.info(
            f"Cliente: **{cliente_sel}** • "
            f"{'IDs: ' + ', '.join(sorted(set(subset_preview['IDLancFiado'].astype(str)))) if not subset_preview.empty else ''} • "
            f"Total bruto selecionado: **{_fmt_brl(total_sel)}**"
        )

        if usar_cartao:
            with st.expander("💳 Detalhes da maquininha (informe o LÍQUIDO)", expanded=True):
                cdc1, cdc2 = st.columns([1,1])
                with cdc1:
                    valor_liquido_cartao = st.number_input(
                        "Valor recebido (líquido da maquininha)",
                        value=float(total_sel),
                        step=1.0, format="%.2f"
                    )
                    bandeira_cartao = st.selectbox(
                        "Bandeira", ["", "Visa", "Mastercard", "Maestro", "Elo", "Hipercard", "Amex", "Outros"], index=0
                    )
                with cdc2:
                    tipo_cartao = st.selectbox("Tipo", ["Débito", "Crédito"], index=1)
                    parcelas_cartao = st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)

                taxa_valor_est = max(0.0, float(total_sel) - float(valor_liquido_cartao or 0.0))
                taxa_pct_est = (taxa_valor_est / float(total_sel) * 100.0) if total_sel > 0 else 0.0
                st.metric("Taxa estimada", _fmt_brl(taxa_valor_est), _fmt_pct(taxa_pct_est))

        resumo_srv = (
            subset_preview.groupby("Serviço", as_index=False)
            .agg(Qtd=("Serviço","count"), Total=("Valor","sum"))
            .sort_values(["Qtd", "Total"], ascending=[False, False])
        )
        resumo_srv["Total"] = resumo_srv["Total"].map(_fmt_brl)
        st.caption("Resumo por serviço selecionado:")
        st.dataframe(resumo_srv, use_container_width=True, hide_index=True)

    tem_selecao = bool(id_selecionados) if modo_sel.startswith("Por ID") else bool(linhas_indices_sel)
    disabled_btn = not (cliente_sel and tem_selecao and forma_pag)

    if st.button("Registrar pagamento", use_container_width=True, disabled=disabled_btn):
        dfb, ws_base2 = read_base_raw(ss)
        ensure_headers(ws_base2, BASE_COLS_ALL)
        format_extras_numeric(ws_base2)

        if modo_sel.startswith("Por ID"):
            mask = dfb.get("IDLancFiado", "").isin(id_selecionados)
        else:
            mask = dfb.index.isin(linhas_indices_sel)

        if not mask.any():
            st.error("Nenhuma linha encontrada para a seleção feita.")
        else:
            subset_all = dfb[mask].copy()
            subset_all["Valor"] = pd.to_numeric(subset_all["Valor"], errors="coerce").fillna(0)
            total_bruto = float(subset_all["Valor"].sum())
            data_pag_str = data_pag.strftime(DATA_FMT)

            id_pag = f"P-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}"
            if usar_cartao and (valor_liquido_cartao is not None):
                total_liquido = float(valor_liquido_cartao or 0.0)
            else:
                total_liquido = total_bruto
            taxa_total_valor = max(0.0, total_bruto - total_liquido)
            taxa_total_pct   = (taxa_total_valor / total_bruto * 100.0) if total_bruto > 0 else 0.0

            headers_map = col_map(ws_base2)
            updates, liq_acum = [], 0.0
            idxs = list(subset_all.index)
            for i, idx in enumerate(idxs):
                row_no = int(idx) + 2
                bruto_i = float(subset_all.loc[idx, "Valor"])
                if total_bruto > 0:
                    liq_i = round(total_liquido * (bruto_i / total_bruto), 2)
                else:
                    liq_i = 0.0
                if i == len(idxs) - 1:
                    liq_i = round(total_liquido - liq_acum, 2)
                liq_acum += liq_i
                taxa_i  = round(bruto_i - liq_i, 2)
                taxa_pct_i = (taxa_i / bruto_i * 100.0) if bruto_i > 0 else 0.0

                pairs = {
                    "Conta": forma_pag,
                    "StatusFiado": "Pago",
                    "VencimentoFiado": "",
                    "DataPagamento": data_pag_str,
                    "Valor": liq_i,
                    "ValorBrutoRecebido": (bruto_i if usar_cartao else ""),
                    "ValorLiquidoRecebido": (liq_i if usar_cartao else ""),
                    "TaxaCartaoValor": (taxa_i if usar_cartao else ""),
                    "TaxaCartaoPct": (round(taxa_pct_i, 4) if usar_cartao else ""),
                    "FormaPagDetalhe": (f"{(bandeira_cartao or '-')} | {tipo_cartao} | {int(parcelas_cartao)}x" if usar_cartao else ""),
                    "PagamentoID": id_pag
                }
                for col, val in pairs.items():
                    c = headers_map.get(_norm_key(col))
                    if c:
                        updates.append({"range": rowcol_to_a1(row_no, c), "values": [[val]]})

            # Aplica updates de pagamento
            if updates:
                ws_base2.batch_update(updates, value_input_option="USER_ENTERED")

            # 💝 CaixinhaDia — grava SEMPRE na MESMA LINHA (sem criar linhas novas)
            #  • Por ID: primeira linha de cada ID selecionado
            #  • Por linha: primeira linha dentre as selecionadas
            if caixinha_dia_val and float(caixinha_dia_val) > 0:
                col_cx = headers_map.get(_norm_key("CaixinhaDia"))
                updates_cx = []
                if col_cx:
                    if modo_sel.startswith("Por ID"):
                        for idl in sorted(set(subset_all["IDLancFiado"].astype(str))):
                            linhas_id = subset_all[subset_all["IDLancFiado"].astype(str) == idl]
                            if not linhas_id.empty:
                                idx_primeiro = int(linhas_id.index[0])
                                row_no = idx_primeiro + 2
                                updates_cx.append({
                                    "range": rowcol_to_a1(row_no, col_cx),
                                    "values": [[float(caixinha_dia_val)]],
                                })
                    else:
                        idx_primeiro = int(subset_all.index[0])
                        row_no = idx_primeiro + 2
                        updates_cx.append({
                            "range": rowcol_to_a1(row_no, col_cx),
                            "values": [[float(caixinha_dia_val)]],
                        })

                if updates_cx:
                    ws_base2.batch_update(updates_cx, value_input_option="USER_ENTERED")

            # Registros auxiliares (taxas e pagamentos)
            if usar_cartao:
                try:
                    ws_taxas = garantir_aba(ss, ABA_TAXAS, TAXAS_COLS)
                    ensure_headers(ws_taxas, TAXAS_COLS)
                    append_rows_generic(ws_taxas, [{
                        "IDPagamento": id_pag,
                        "Cliente": cliente_sel,
                        "DataPag": data_pag_str,
                        "Bandeira": bandeira_cartao,
                        "Tipo": tipo_cartao,
                        "Parcelas": int(parcelas_cartao),
                        "Bruto": total_bruto,
                        "Liquido": total_liquido,
                        "TaxaValor": round(taxa_total_valor, 2),
                        "TaxaPct": round(taxa_total_pct, 4),
                        "IDLancs": ";".join(sorted(set(subset_all["IDLancFiado"].astype(str))))
                    }], default_headers=TAXAS_COLS)
                except Exception:
                    pass

            ws_p = garantir_aba(ss, ABA_PAGT, PAGT_COLS)
            ensure_headers(ws_p, PAGT_COLS)
            append_rows_generic(ws_p, [{
                "IDPagamento": id_pag,
                "IDLancs": ";".join(sorted(set(subset_all["IDLancFiado"].astype(str)))),
                "DataPagamento": data_pag_str,
                "Cliente": cliente_sel,
                "Forma": forma_pag,
                "TotalLiquido": total_liquido,
                "Obs": (obs or ""),
                "TotalBruto": total_bruto,
                "TaxaValor": round(taxa_total_valor, 2),
                "TaxaPct": round(taxa_total_pct, 4),
            }], default_headers=PAGT_COLS)

            st.success(
                f"Pagamento registrado para **{cliente_sel}**. "
                f"Total líquido: {_fmt_brl(total_liquido)} (bruto {_fmt_brl(total_bruto)})."
                + (f" 💝 Caixinha gravada: {_fmt_brl(float(caixinha_dia_val))}" if caixinha_dia_val and float(caixinha_dia_val)>0 else "")
            )
            st.cache_data.clear()

            # ---- Mensagens (quitado + cópia enriquecida)
            try:
                # -------- Card 1: ✅ Fiado quitado (competência)
                ids_lanc_set = sorted(set(subset_all["IDLancFiado"].astype(str)))
                ids_lanc_txt = "; ".join(ids_lanc_set)
                servicos_txt = servicos_compactos_por_ids_parcial(subset_all)

                msg_quit = (
                    "✅ <b>Fiado quitado (competência)</b>\n"
                    f"👤 Cliente: <b>{cliente_sel}</b>\n"
                    f"🧰 Serviço(s): <b>{servicos_txt}</b>\n"
                    f"💳 Forma: <b>{forma_pag}</b>\n"
                    f"🧾 Bruto: <b>{_fmt_brl(total_bruto)}</b>\n"
                    f"💵 Líquido: <b>{_fmt_brl(total_liquido)}</b>\n"
                    f"📅 Data pagto: <b>{data_pag_str}</b>\n"
                    f"🆔 IDs: <code>{ids_lanc_txt}</code>\n"
                    f"🧾 Pag.: <code>{id_pag}</code>"
                    + (f"\n📝 Obs.: {obs}" if obs else "")
                )

                foto_cli = FOTOS.get(_norm(cliente_sel))

                # Destinos sem duplicar: sempre JP + canal do Vinicius quando for atendimento dele
                destinos = {_get_chat_id_jp()}  # set dedup
                funcs_set = sorted(set(subset_all.get("Funcionário", "").astype(str).str.strip()))

                chat_func = None
                if len(funcs_set) == 1:
                    chat_func = _chat_id_por_func(funcs_set[0])
                    if chat_func and chat_func != _get_chat_id_jp():
                        destinos.add(chat_func)

                for dest in destinos:
                    if not dest:
                        continue
                    if foto_cli:
                        tg_send_photo(foto_cli, msg_quit, chat_id=dest)
                    else:
                        tg_send(msg_quit, chat_id=dest)

                # -------- Card 2: Cópia para controle (enriquecida)
                datas_sel = pd.to_datetime(subset_all["Data"], format=DATA_FMT, errors="coerce").dropna().dt.date
                periodos = [p for p in subset_all.get("Período","").astype(str).tolist() if p.strip()]
                funcs    = [f for f in subset_all.get("Funcionário","").astype(str).tolist() if f.strip()]

                if len(set(datas_sel)) == 1:
                    data_atend_txt = next(iter(set(datas_sel))).strftime(DATA_FMT)
                elif len(set(datas_sel)) > 1:
                    dmin = min(datas_sel).strftime(DATA_FMT); dmax = max(datas_sel).strftime(DATA_FMT)
                    data_atend_txt = f"{dmin} → {dmax}"
                else:
                    data_atend_txt = "-"

                periodo_txt = (periodos[0] if len(set(periodos)) == 1 else "—")
                atendido_por_txt = (funcs[0] if len(set(funcs)) == 1 else ", ".join(sorted(set(funcs))))

                df_priv, _ = read_base_raw(conectar_sheets())

                def _resumo_visitas(df_base: pd.DataFrame, cliente: str):
                    if df_base is None or df_base.empty or not cliente: return None, None, 0, "-"
                    df = df_base.copy()
                    df["__dt"] = pd.to_datetime(df.get("Data"), format=DATA_FMT, errors="coerce")
                    df = df[(df.get("Cliente","").astype(str).str.strip()==str(cliente).strip()) & df["__dt"].notna()]
                    if df.empty: return None, None, 0, "-"
                    df_day = df.sort_values("__dt").groupby("__dt", as_index=False).agg({"Funcionário":"last"})
                    datas = df_day["__dt"].sort_values().tolist()
                    total = len(datas)
                    ultimo_func = str(df_day.iloc[-1]["Funcionário"]) if total else "-"
                    gaps = [(datas[i]-datas[i-1]).days for i in range(1,total)]
                    media = (sum(gaps)/len(gaps)) if gaps else None
                    dist = (today_local()-datas[-1].date()).days if total else None
                    return media, dist, total, (ultimo_func or "-")

                media_dias, dist_ult, total_atends, ult_func = _resumo_visitas(df_priv, cliente_sel)

                bloco_atend = (
                    "📌 <b>Atendimento registrado</b>\n"
                    f"👤 Cliente: <b>{cliente_sel}</b>\n"
                    f"📅 Data: <b>{data_atend_txt}</b>\n"
                    f"🕑 Período: <b>{periodo_txt or '-'}</b>\n"
                    f"✂️ Serviço(s): <b>{servicos_txt}</b>\n"
                    f"🧑‍🤝‍🧑 Atendido por: <b>{atendido_por_txt or '-'}</b>"
                )

                linha_taxa_cp = (f"\n🧾 Taxa total: <b>{_fmt_brl(taxa_total_valor)} ({_fmt_pct(taxa_total_pct)})</b>" if usar_cartao else "")

                # 💝 Caixinha no card (só se todas as linhas são do mesmo dia)
                linha_caixinha = ""
                if len(set(datas_sel)) == 1:
                    unico_dia = next(iter(set(datas_sel))).strftime(DATA_FMT)
                    d_cx = df_priv[
                        (df_priv.get("Cliente","").astype(str).str.strip()==str(cliente_sel).strip()) &
                        (df_priv.get("Data","").astype(str).str.strip()==unico_dia)
                    ].copy()
                    if "CaixinhaDia" in d_cx.columns:
                        v_cx = pd.to_numeric(d_cx["CaixinhaDia"], errors="coerce").fillna(0).sum()
                        if v_cx > 0:
                            linha_caixinha = f"\n💝 Caixinha: <b>{_fmt_brl(float(v_cx))}</b>"

                bloco_hist_indic = (
                    "\n\n📊 <b>Histórico</b>\n"
                    f"• Média: <b>{(f'{media_dias:.1f} dias' if media_dias is not None else '-') }</b>\n"
                    f"• Distância da última: <b>{(f'{int(dist_ult)} dias' if dist_ult is not None else '-') }</b>\n"
                    f"• Total de atendimentos: <b>{int(total_atends)}</b>\n"
                    f"• Último atendente: <b>{ult_func}</b>"
                )

                hist = historico_cliente_por_ano(df_priv, cliente_sel)
                if hist:
                    anos_ord = sorted(hist.keys(), reverse=True)
                    linhas_hist = "\n".join(f"• {ano}: <b>{_fmt_brl(hist[ano])}</b>" for ano in anos_ord)
                    bloco_hist = "\n\n📚 <b>Histórico por ano</b>\n" + linhas_hist
                else:
                    bloco_hist = "\n\n📚 <b>Histórico por ano</b>\n• (sem registros)"

                ano_corr = today_local().year
                brk, tq, tv, oq, ov = breakdown_por_servico_no_ano(df_priv, cliente_sel, ano_corr, max_itens=8)
                if not brk.empty:
                    linhas_srv = "\n".join(
                        f"• {r['Serviço']}: {int(r['Qtd'])}× · <b>{_fmt_brl(float(r['Total']))}</b>"
                        for _, r in brk.iterrows()
                    )
                    if oq > 0:
                        linhas_srv += f"\n• Outros: {oq}× · <b>{_fmt_brl(ov)}</b>"
                    bloco_srv = f"\n\n🔎 <b>{ano_corr}: por serviço</b>\n{linhas_srv}\nTotal ({ano_corr}): <b>{_fmt_brl(tv)}</b>"
                else:
                    bloco_srv = f"\n\n🔎 <b>{ano_corr}: por serviço</b>\n• (sem registros)"

                # Frequência por funcionário (visitas por dia)
                freq_lines = ""
                try:
                    df_vis = df_priv.copy()
                    df_vis["__dt"] = pd.to_datetime(df_vis.get("Data"), format=DATA_FMT, errors="coerce")
                    df_vis = df_vis[(df_vis["__dt"].notna()) & (df_vis.get("Cliente","")==cliente_sel)]
                    if not df_vis.empty:
                        df_day = df_vis.sort_values("__dt").groupby("__dt", as_index=False).agg({"Funcionário":"last"})
                        vc = df_day["Funcionário"].astype(str).value_counts()
                        itens = [f"• {k}: <b>{int(v)}</b> visita(s)" for k, v in vc.items()]
                        if itens:
                            freq_lines = "\n\n📊 <b>Frequência por funcionário</b>\n" + "\n".join(itens)
                except Exception:
                    pass

                msg_jp = (
                    "🧾 <b>Cópia para controle</b>\n" + bloco_atend +
                    f"\n💳 Forma: <b>{forma_pag}</b>\n"
                    f"💵 Bruto: <b>{_fmt_brl(total_bruto)}</b> · Líquido: <b>{_fmt_brl(total_liquido)}</b>"
                    + linha_taxa_cp + linha_caixinha +
                    bloco_hist_indic + bloco_hist + bloco_srv + freq_lines +
                    (f"\n\n📝 Obs.: {obs}" if obs else "")
                )

                if foto_cli: tg_send_photo(foto_cli, msg_jp, chat_id=_get_chat_id_jp())
                else:        tg_send(msg_jp, chat_id=_get_chat_id_jp())

            except Exception:
                pass

            # Comissões
            try:
                sub = subset_all.copy()
                sub["Valor"] = pd.to_numeric(sub["Valor"], errors="coerce").fillna(0.0)
                grup = sub.groupby("Funcionário", dropna=True)["Valor"].sum().reset_index()
                itens = []
                for _, r in grup.iterrows():
                    func_raw = str(r["Funcionário"]).strip()
                    if unicodedata.normalize("NFKC", func_raw).casefold() not in COMISSAO_FUNCIONARIOS:
                        continue
                    comiss = round(float(r["Valor"]) * COMISSAO_PERC_PADRAO, 2)
                    itens.append(f"• {func_raw}: <b>{_fmt_brl(comiss)}</b>")
                if itens:
                    dt_pgto = proxima_terca(data_pag)
                    tg_send(
                        "💸 <b>Comissões sugeridas</b> "
                        f"({int(COMISSAO_PERC_PADRAO*100)}%)\n" + "\n".join(itens) +
                        f"\n📌 Pagar na próxima terça: <b>{dt_pgto.strftime(DATA_FMT)}</b>",
                        chat_id=_get_chat_id_jp()
                    )
            except Exception:
                pass

# ---------- 3) Em aberto & exportação ----------
elif acao == "📋 Em aberto & exportação":
    st.subheader("📋 Fiados em aberto (agrupados por ID)")
    ss = conectar_sheets()
    df_base_full, _ = read_base_raw(ss)

    if df_base_full.empty:
        st.info("Sem dados.")
    else:
        em_aberto = df_base_full[df_base_full.get("StatusFiado","") == "Em aberto"].copy()
        if em_aberto.empty:
            st.success("Nenhum fiado em aberto 🎉")
        else:
            colf1, colf2 = st.columns([2,1])
            with colf1:
                filtro_cliente = st.text_input("Filtrar por cliente (opcional)", "")
                if filtro_cliente.strip():
                    em_aberto = em_aberto[
                        em_aberto["Cliente"].astype(str).str.contains(filtro_cliente.strip(), case=False, na=False)
                    ]
            with colf2:
                funcionarios_abertos = sorted(
                    em_aberto["Funcionário"].dropna().astype(str).unique().tolist()
                )
                filtro_func = st.selectbox("Filtrar por funcionário (opcional)", [""] + funcionarios_abertos)
                if filtro_func:
                    em_aberto = em_aberto[em_aberto["Funcionário"] == filtro_func]

            hoje = today_local()
            def parse_dt(x):
                try:
                    return datetime.strptime(str(x), DATA_FMT).date()
                except Exception:
                    return None
            em_aberto["__venc"] = em_aberto["VencimentoFiado"].apply(parse_dt)
            em_aberto["DiasAtraso"] = em_aberto["__venc"].apply(
                lambda d: (hoje - d).days if (d is not None and hoje > d) else 0
            )
            em_aberto["Situação"] = em_aberto["DiasAtraso"].apply(lambda n: "Em dia" if n<=0 else f"{int(n)}d atraso")

            em_aberto["Valor"] = pd.to_numeric(em_aberto["Valor"], errors="coerce").fillna(0)
            resumo = (
                em_aberto.groupby(["IDLancFiado","Cliente"], as_index=False)
                .agg(ValorTotal=("Valor","sum"), QtdeServicos=("Serviço","count"),
                     Combo=("Combo","first"), MaxAtraso=("DiasAtraso","max"))
            )
            resumo["Situação"] = resumo["MaxAtraso"].apply(lambda n: "Em dia" if n<=0 else f"{int(n)}d atraso")

            resumo["RegistradoEm"] = resumo["IDLancFiado"].apply(
                lambda x: (data_reg_do_id(x).strftime(DATA_FMT) if data_reg_do_id(x) else "-")
            )
            resumo["Periodo"] = resumo["IDLancFiado"].apply(lambda x: periodo_do_id(em_aberto, x))

            st.dataframe(
                resumo.sort_values(["MaxAtraso","ValorTotal"], ascending=[False, False])[[
                    "IDLancFiado","RegistradoEm","Periodo","Cliente","ValorTotal","QtdeServicos","Combo","Situação"
                ]],
                use_container_width=True, hide_index=True
            )

            total = float(resumo["ValorTotal"].sum())
            st.metric("Total em aberto", _fmt_brl(total))

            try:
                from openpyxl import Workbook  # noqa
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_excel(
                        w, index=False, sheet_name="Fiado_Em_Aberto"
                    )
                st.download_button("⬇️ Exportar (Excel)", data=buf.getvalue(), file_name="fiado_em_aberto.xlsx")
            except Exception:
                csv_bytes = em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_csv(
                    index=False
                ).encode("utf-8-sig")
                st.download_button("⬇️ Exportar (CSV)", data=csv_bytes, file_name="fiado_em_aberto.csv")

# ---------- 4) Pagos (histórico) ----------
else:  # acao == "📗 Pagos (histórico)"
    st.subheader("📗 Fiados pagos — histórico consolidado")

    ss = conectar_sheets()

    df_base, _ = read_base_raw(ss)
    df_pagos_base = df_base[df_base.get("StatusFiado", "") == "Pago"].copy()

    ws_p = garantir_aba(ss, ABA_PAGT, PAGT_COLS)
    ensure_headers(ws_p, PAGT_COLS)
    df_pag = get_as_dataframe(ws_p, evaluate_formulas=True, header=0).fillna("")
    df_pag.columns = [str(c).strip() for c in df_pag.columns]
    df_pag = df_pag.loc[:, ~pd.Index(df_pag.columns).duplicated(keep="first")]

    def _to_date(s):
        try:
            return datetime.strptime(str(s), DATA_FMT).date()
        except Exception:
            return None

    if "DataPagamento" in df_pag.columns:
        df_pag["__data"] = df_pag["DataPagamento"].apply(_to_date)
    else:
        df_pag["__data"] = None

    c1, c2, c3 = st.columns([1,1,2])
    with c1:
        data_ini = st.date_input("De", value=today_local() - timedelta(days=30), key="pagos_ini")
    with c2:
        data_fim = st.date_input("Até", value=today_local(), key="pagos_fim")
    with c3:
        filtro_cli = st.text_input("Cliente (contém)", "", key="pagos_cli")

    mask = pd.Series([True] * len(df_pag))
    if data_ini:
        mask &= (df_pag["__data"] >= data_ini)
    if data_fim:
        mask &= (df_pag["__data"] <= data_fim)
    if filtro_cli.strip():
        mask &= df_pag.get("Cliente", "").astype(str).str.contains(filtro_cli.strip(), case=False, na=False)

    vis = df_pag[mask].copy()

    def _registrado_de(idl_str: str) -> str:
        ids = [s.strip() for s in str(idl_str or "").split(";") if s.strip()]
        datas = [data_reg_do_id(s) for s in ids if data_reg_do_id(s)]
        return min(datas).strftime(DATA_FMT) if datas else "-"
    vis["RegistradoDe"] = vis.get("IDLancs", "").apply(_registrado_de)

    try:
        totals = {
            "Total líquido": vis.get("TotalLiquido", 0).apply(pd.to_numeric, errors="coerce").fillna(0).sum(),
            "Total bruto": vis.get("TotalBruto", 0).apply(pd.to_numeric, errors="coerce").fillna(0).sum(),
            "Taxa": vis.get("TaxaValor", 0).apply(pd.to_numeric, errors="coerce").fillna(0).sum(),
        }
    except Exception:
        totals = {"Total líquido": 0.0, "Total bruto": 0.0, "Taxa": 0.0}

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pagamentos", f"{len(vis)}")
    k2.metric("Total líquido", _fmt_brl(float(totals["Total líquido"])))
    k3.metric("Total bruto", _fmt_brl(float(totals["Total bruto"])))
    k4.metric("Taxas", _fmt_brl(float(totals["Taxa"])))

    cols_show = ["DataPagamento","RegistradoDe","Cliente","Forma","TotalBruto","TotalLiquido","TaxaValor","TaxaPct","IDLancs","Obs","IDPagamento"]
    cols_show = [c for c in cols_show if c in vis.columns]
    st.dataframe(
        vis[cols_show].sort_values("DataPagamento", ascending=False),
        use_container_width=True,
        hide_index=True
    )

    # Export: tenta Excel; CSV sempre disponível
    try:
        from openpyxl import Workbook  # noqa
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            vis[cols_show].sort_values("DataPagamento", ascending=False).to_excel(
                w, index=False, sheet_name="Fiados_Pagos"
            )
        st.download_button("⬇️ Exportar (Excel)", data=buf.getvalue(), file_name="fiados_pagos.xlsx")
    except Exception:
        pass
    csv_bytes = vis[cols_show].sort_values("DataPagamento", ascending=False).to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Exportar (CSV)", data=csv_bytes, file_name="fiados_pagos.csv")

    st.markdown("---")
    st.caption("🔎 Detalhe rápido por cliente (no período filtrado)")
    cli_pick = st.selectbox("Cliente", [""] + sorted(vis.get("Cliente","").astype(str).unique().tolist()))
    if cli_pick:
        colf1, colf2 = st.columns([1,3])
        with colf1:
            show_foto_cliente(cli_pick)
        with colf2:
            linhas_cli = df_pagos_base[
                (df_pagos_base.get("Cliente","") == cli_pick)
                & df_pagos_base.get("DataPagamento","").astype(str).str.len().gt(0)
            ].copy()
            if not linhas_cli.empty:
                linhas_cli["Valor"] = pd.to_numeric(linhas_cli["Valor"], errors="coerce").fillna(0.0)
                st.dataframe(
                    linhas_cli[["Data","Serviço","Valor","Conta","Funcionário","IDLancFiado","DataPagamento"]],
                    use_container_width=True,
                    hide_index=True
                )
                st.metric("Total (linhas pagas — detalhe)", _fmt_brl(float(linhas_cli["Valor"].sum())))
            else:
                st.info("Sem linhas pagas na BASE para esse cliente no período.")
