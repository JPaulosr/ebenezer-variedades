# pages/01_produtos.py
# -*- coding: utf-8 -*-
import re, json, math, time
from collections.abc import Mapping

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG / COLUNAS
# =========================
st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos — Ebenezér Variedades")

ABA_PRODUTOS = "Produtos"
COLS_PRODUTOS = [
    "ID", "Nome", "Categoria", "Unidade", "Fornecedor",
    "CustoAtual", "PreçoVenda", "Markup %", "Margem %",
    "EstoqueAtual", "EstoqueMin", "LeadTimeDias", "Ativo?"
]

# =========================
# HELPERS: SHEET ID (aceita ID ou URL)
# =========================
def _extract_sheet_id(url_or_id: str) -> str:
    if not url_or_id:
        return ""
    m = re.search(r"/d/([A-Za-z0-9\-_]+)", url_or_id)
    return (m.group(1) if m else url_or_id).strip()

def get_sheet_id_from_secrets_or_input():
    raw = st.secrets.get("SHEET_ID", "") or st.secrets.get("PLANILHA_URL", "")
    raw = st.text_input("Google Sheet ID ou URL da planilha", value=raw).strip()
    sid = _extract_sheet_id(raw)
    return sid

# =========================
# CONEXÃO GOOGLE SHEETS (sem alterar st.secrets)
# =========================
@st.cache_resource(show_spinner=False)
def conectar_sheets(sheet_id: str):
    svc_raw = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not svc_raw:
        st.error("🚫 Faltam credenciais em st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()

    # Aceita dict (TOML) ou string JSON; sempre cria uma CÓPIA mutável
    if isinstance(svc_raw, str):
        try:
            svc = json.loads(svc_raw)  # -> dict
        except Exception:
            st.error("❌ GCP_SERVICE_ACCOUNT está como string, mas não é JSON válido.")
            st.stop()
    elif isinstance(svc_raw, Mapping):
        svc = dict(svc_raw)           # -> cópia mutável
    else:
        st.error("❌ Formato inválido em GCP_SERVICE_ACCOUNT.")
        st.stop()

    # Normaliza private_key (corrige '\\n' literais -> quebras reais)
    pk = svc.get("private_key", "")
    if not isinstance(pk, str) or not pk.strip():
        st.error("❌ 'private_key' ausente nas credenciais.")
        st.stop()
    pk = pk.strip()
    if "\\n" in pk and "\n" not in pk:
        pk = pk.replace("\\n", "\n")
    if "BEGIN PRIVATE KEY" not in pk or "END PRIVATE KEY" not in pk:
        st.error("❌ Formato da 'private_key' inválido. Use o bloco completo BEGIN/END com quebras de linha reais.")
        st.stop()
    svc["private_key"] = pk  # em 'svc' (cópia), não em st.secrets

    # Campos essenciais
    for k in ("type", "client_email", "token_uri"):
        if not svc.get(k):
            st.error(f"❌ Campo ausente em GCP_SERVICE_ACCOUNT: '{k}'.")
            st.stop()

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

def _obter_aba(sh, nome):
    try:
        return sh.worksheet(nome)
    except gspread.WorksheetNotFound:
        return None

def _criar_aba_produtos(sh):
    ws = sh.add_worksheet(title=ABA_PRODUTOS, rows=1000, cols=max(len(COLS_PRODUTOS)+3, 20))
    ws.update("A1", [COLS_PRODUTOS])
    return ws

def _garantir_estrutura_produtos(sh):
    ws = _obter_aba(sh, ABA_PRODUTOS)
    if ws is None:
        ws = _criar_aba_produtos(sh)
        return ws, True
    vals = ws.get_all_values()
    if not vals or not vals[0] or len(vals[0]) < len(COLS_PRODUTOS):
        ws.update("A1", [COLS_PRODUTOS])
    else:
        old_headers = [h.strip() for h in vals[0]]
        if "SKU" in old_headers or "EAN" in old_headers:
            with st.expander("⚙️ Detectei cabeçalho antigo (SKU/EAN). Clique para ajustar para o novo padrão (ID)."):
                if st.button("Ajustar cabeçalho agora"):
                    _migrar_cabecalho_produtos(ws)
                    st.success("Cabeçalho ajustado para o novo padrão (ID).")
                    st.rerun()
    return ws, False

def _migrar_cabecalho_produtos(ws):
    raw = ws.get_all_values()
    if not raw:
        ws.update("A1", [COLS_PRODUTOS])
        return
    old_cols = raw[0]
    df_old = pd.DataFrame(raw[1:], columns=old_cols)
    df_new = pd.DataFrame(columns=COLS_PRODUTOS)
    # ID preferindo 'ID', senão 'SKU', senão vazio
    if "ID" in df_old.columns:
        df_new["ID"] = df_old["ID"].astype(str)
    elif "SKU" in df_old.columns:
        df_new["ID"] = df_old["SKU"].astype(str)
    else:
        df_new["ID"] = ""
    simple_map = {
        "Nome":"Nome","Categoria":"Categoria","Unidade":"Unidade","Fornecedor":"Fornecedor",
        "CustoAtual":"CustoAtual","PreçoVenda":"PreçoVenda","Markup %":"Markup %","Margem %":"Margem %",
        "EstoqueAtual":"EstoqueAtual","EstoqueMin":"EstoqueMin","LeadTimeDias":"LeadTimeDias","Ativo?":"Ativo?"
    }
    for old, new in simple_map.items():
        df_new[new] = df_old[old] if old in df_old.columns else ""
    # Gera IDs ausentes
    new_ids = _sequenciar_ids(df_new["ID"].tolist())
    df_new["ID"] = [x if str(x).strip() not in ("", "nan", "None") else next(new_ids) for x in df_new["ID"]]
    # Escreve de volta
    values = [df_new.columns.tolist()] + df_new.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", values)

def _sequenciar_ids(existing_ids):
    pad = re.compile(r"PRO-(\d{4})$")
    usados = []
    for x in existing_ids:
        m = pad.match(str(x).strip()) if x else None
        if m:
            try: usados.append(int(m.group(1)))
            except: pass
    base = max(usados) if usados else 0
    def _next():
        nonlocal base
        base += 1
        return f"PRO-{base:04d}"
    return _next()

@st.cache_data(show_spinner=False)
def carregar_df_produtos(sheet_id: str) -> pd.DataFrame:
    sh = conectar_sheets(sheet_id)
    ws, _ = _garantir_estrutura_produtos(sh)
    data = ws.get_all_values()
    if not data:
        return pd.DataFrame(columns=COLS_PRODUTOS)
    df = pd.DataFrame(data[1:], columns=data[0] if data[0] else COLS_PRODUTOS)
    for c in COLS_PRODUTOS:
        if c not in df.columns:
            df[c] = ""
    num_cols = ["CustoAtual","PreçoVenda","Markup %","Margem %","EstoqueAtual","EstoqueMin","LeadTimeDias"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Ativo?"] = df["Ativo?"].fillna("").astype(str).str.upper().str.strip()
    return df[COLS_PRODUTOS]

def proximo_id(df: pd.DataFrame) -> str:
    if df.empty or "ID" not in df.columns:
        return "PRO-0001"
    padrao = re.compile(r"PRO-(\d{4})$")
    numeros = []
    for x in df["ID"].dropna().astype(str):
        m = padrao.match(x.strip())
        if m:
            try: numeros.append(int(m.group(1)))
            except: pass
    n = (max(numeros) + 1) if numeros else 1
    return f"PRO-{n:04d}"

def pct_markup(custo: float, preco: float) -> float:
    if custo is None or preco is None or custo <= 0:
        return float("nan")
    return (preco - custo) / custo

def pct_margem(custo: float, preco: float) -> float:
    if preco is None or preco <= 0:
        return float("nan")
    return (preco - custo) / preco

def sim_nao(flag: bool) -> str:
    return "SIM" if flag else "NÃO"

def user_entered_append(ws, row_values):
    ws.append_row(row_values, value_input_option="USER_ENTERED")

# =========================
# SHEET_ID
# =========================
SHEET_ID = get_sheet_id_from_secrets_or_input()
if not SHEET_ID:
    st.error("Informe o ID ou configure SHEET_ID/PLANILHA_URL em Secrets.")
    st.stop()

# =========================
# CARREGAR + KPIs
# =========================
df = carregar_df_produtos(SHEET_ID)

c1, c2, c3 = st.columns(3)
with c1: st.metric("Itens cadastrados", len(df))
with c2: st.metric("Ativos", int((df["Ativo?"] == "SIM").sum() if not df.empty else 0))
with c3:
    crit = 0
    if not df.empty:
        ea = pd.to_numeric(df["EstoqueAtual"], errors="coerce")
        em = pd.to_numeric(df["EstoqueMin"], errors="coerce")
        crit = int(((ea <= em) & (df["Ativo?"] == "SIM")).sum())
    st.metric("Estoque crítico", crit)

st.divider()

# =========================
# LISTA / FILTROS
# =========================
st.subheader("🔎 Lista de produtos")
col1, col2, col3 = st.columns([2,1,1])
with col1:
    termo = st.text_input("Buscar por nome / fornecedor / categoria", "")
with col2:
    categorias = sorted([c for c in df["Categoria"].dropna().unique().tolist() if str(c).strip()])
    categoria = st.selectbox("Categoria", options=["(todas)"] + categorias, index=0)
with col3:
    status = st.selectbox("Status", ["(todos)", "Ativos", "Inativos"], index=1)

df_view = df.copy()
if termo:
    t = termo.strip().lower()
    df_view = df_view[
        df_view["Nome"].astype(str).str.lower().str.contains(t)
        | df_view["Fornecedor"].astype(str).str.lower().str.contains(t)
        | df_view["Categoria"].astype(str).str.lower().str.contains(t)
    ]
if categoria != "(todas)":
    df_view = df_view[df_view["Categoria"].astype(str) == categoria]
if status == "Ativos":
    df_view = df_view[df_view["Ativo?"] == "SIM"]
elif status == "Inativos":
    df_view = df_view[df_view["Ativo?"] == "NÃO"]

st.dataframe(df_view.reset_index(drop=True), use_container_width=True, height=420)
st.divider()

# =========================
# FORM: ADICIONAR PRODUTO
# =========================
st.subheader("➕ Adicionar novo produto")

with st.form("form_add_prod"):
    a, b = st.columns([2,1])
    with a:
        nome = st.text_input("Nome *", "")
        categoria_n = st.text_input("Categoria", "")
        fornecedor = st.text_input("Fornecedor", "")
    with b:
        unidade = st.selectbox("Unidade", ["un", "L", "kg", "pct", "cx", "mL", "g"], index=0)
        estoque_atual = st.number_input("EstoqueAtual", min_value=0.0, value=0.0, step=1.0)
        estoque_min = st.number_input("EstoqueMin", min_value=0.0, value=0.0, step=1.0)
        leadtime = st.number_input("LeadTimeDias", min_value=0, value=3, step=1)

    c, d = st.columns(2)
    with c:
        custo = st.number_input("CustoAtual (R$) *", min_value=0.0, value=0.0, step=0.01, format="%.2f")
    with d:
        preco = st.number_input("PreçoVenda (R$) *", min_value=0.0, value=0.0, step=0.01, format="%.2f")

    mk = pct_markup(float(custo), float(preco)) if (custo or preco) else float("nan")
    mg = pct_margem(float(custo), float(preco)) if (custo or preco) else float("nan")

    e, f, g = st.columns([1,1,1])
    with e: st.caption("Markup % (auto)"); st.write("**" + (f"{mk*100:.2f} %" if not math.isnan(mk) else "—") + "**")
    with f: st.caption("Margem % (auto)"); st.write("**" + (f"{mg*100:.2f} %" if not math.isnan(mg) else "—") + "**")
    with g: ativo_flag = st.checkbox("Ativo?", value=True)

    erro = None
    if st.form_submit_button("Salvar produto", use_container_width=True):
        if not nome.strip():
            erro = "Informe o **Nome**."
        elif float(preco) <= 0 or float(custo) < 0:
            erro = "Preencha **CustoAtual** e **PreçoVenda** válidos (Preço > 0)."
        elif not df.empty and (df["Nome"].str.lower().str.strip() == nome.lower().strip()).any():
            erro = "Já existe um produto com esse **Nome**. Altere o nome ou edite o existente."

        if erro:
            st.error(erro)
        else:
            try:
                sh = conectar_sheets(SHEET_ID)
                ws, _ = _garantir_estrutura_produtos(sh)
                df_atual = carregar_df_produtos(SHEET_ID)
                novo_id = proximo_id(df_atual)
                linha = [
                    novo_id, nome.strip(), categoria_n.strip(), unidade, fornecedor.strip(),
                    float(custo), float(preco),
                    (pct_markup(float(custo), float(preco)) if float(custo) > 0 else ""),
                    (pct_margem(float(custo), float(preco)) if float(preco) > 0 else ""),
                    float(estoque_atual), float(estoque_min), int(leadtime),
                    "SIM" if ativo_flag else "NÃO",
                ]
                ws.append_row(linha, value_input_option="USER_ENTERED")
                st.success(f"✅ Produto **{nome}** salvo com ID **{novo_id}**.")
                carregar_df_produtos.clear()
                time.sleep(0.3)
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erro ao salvar: {e}")

# =========================
# DIAGNÓSTICO
# =========================
with st.expander("🔎 Diagnóstico de credenciais e acesso"):
    if st.button("Testar credenciais e listar abas"):
        try:
            svc = st.secrets.get("GCP_SERVICE_ACCOUNT", {})
            fmt_ok = isinstance(svc, (dict, str))
            st.write("GCP_SERVICE_ACCOUNT é dict/JSON válido?", fmt_ok)
            if isinstance(svc, dict):
                pk = svc.get("private_key", "")
            elif isinstance(svc, str):
                pk = json.loads(svc).get("private_key", "")
            else:
                pk = ""
            st.write("private_key começa com '-----BEGIN'?", str(pk).strip().startswith("-----BEGIN"))
            st.write("private_key termina com 'END PRIVATE KEY-----'?", str(pk).strip().endswith("END PRIVATE KEY-----"))
            sh = conectar_sheets(SHEET_ID)
            st.success("Conexão com Sheets ✅")
            st.write("Abas:", [w.title for w in sh.worksheets()])
        except Exception as e:
            st.error(f"Falhou: {e}")
